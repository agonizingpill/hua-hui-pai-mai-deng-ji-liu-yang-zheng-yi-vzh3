import os
import redis
from sqlalchemy import select,func
from fastapi import HTTPException
from .models import Consignment,AuctionLot,Deal,VaseSample,Dispute,DisputeEvidence,Settlement
from .common import lock,pack,add,audit
WEIGHT={'A':100,'B':90,'C':75};RANK={'A':0,'B':1,'C':2}
PENDING=('estimating','observing','opened')
q=redis.Redis.from_url(os.environ['REDIS_URL'],decode_responses=True)
def lock_deal(s,id):
    pre=s.get(Deal,id)
    if not pre:raise HTTPException(404,'成交单不存在')
    lot=s.get(AuctionLot,pre.lot_id);lock(s,Consignment,lot.consignment_id);return lock(s,Deal,id),lot
def enqueue(s,x):
    x.state='queued';x.error=None;s.flush()
    try:q.rpush('floral-grade',x.id)
    except Exception:x.state='failed';x.error='队列不可用，恢复后可接着办补证'
def open_dispute(s,d,u):
    deal,lot=lock_deal(s,d.deal_id);sample=s.get(VaseSample,lot.sample_id)
    if s.scalar(select(Dispute.id).where(Dispute.deal_id==deal.id,Dispute.state.in_(PENDING))):raise HTTPException(409,'已有未复判争议')
    if sample.observed_days is not None:
        audit(s,u,'提交留样争议');return add(s,Dispute(**d.model_dump(),sample_snapshot=pack(sample)))
    if not sample.prediction:raise HTTPException(409,'原留样无法实测且无寿命估值，先送交寿命估级再申请争议')
    pool=s.scalars(select(VaseSample).where(VaseSample.consignment_id==lot.consignment_id,VaseSample.id!=sample.id).order_by(VaseSample.id).with_for_update()).all()
    if not pool:raise HTTPException(409,'原留样无法实测且同批无备用留样，不能受理替代复判')
    claim=Dispute(**d.model_dump(),mode='proxy',state='estimating',sample_snapshot=pack(sample));s.add(claim);s.flush()
    for x in pool:
        s.add(DisputeEvidence(dispute_id=claim.id,sample_id=x.id))
        if not x.prediction:enqueue(s,x)
    advance(s,claim);audit(s,u,'原留样无法实测，圈定同批备用留样受理替代争议');return pack(claim)
def advance(s,claim):
    claim=lock(s,Dispute,claim.id)
    if claim.state=='estimating':
        rows=s.scalars(select(DisputeEvidence).where(DisputeEvidence.dispute_id==claim.id).order_by(DisputeEvidence.id)).all()
        samples={r.sample_id:s.get(VaseSample,r.sample_id)for r in rows}
        if rows and all(samples[r.sample_id].prediction for r in rows):
            base=claim.sample_snapshot['prediction']['vase_days']
            for r in rows:r.estimate_days=samples[r.sample_id].prediction['vase_days'];r.gap_days=round(abs(r.estimate_days-base),3)
            best=min(rows,key=lambda r:(r.gap_days,r.id))
            for r in rows:
                r.selected=r.id==best.id
                r.reason=f"估值{r.estimate_days}天，距原样估值{base}天差{r.gap_days}天，"+('最为接近，锁定为对照留样'if r.selected else '非最接近，未锁定')
            claim.proxy_sample_id=best.sample_id;claim.state='observing'
    if claim.state=='observing':
        proxy=s.get(VaseSample,claim.proxy_sample_id)
        if proxy.observed_days is not None:
            claim.corrected_days=round(claim.sample_snapshot['prediction']['vase_days']+(proxy.observed_days-proxy.prediction['vase_days']),2)
            row=s.scalar(select(DisputeEvidence).where(DisputeEvidence.dispute_id==claim.id,DisputeEvidence.sample_id==proxy.id))
            if row:row.observed_days=proxy.observed_days
            claim.state='opened'
    return claim
def resume(s,id,u):
    pre=s.get(Dispute,id)
    if not pre:raise HTTPException(404,'争议不存在')
    if pre.state not in PENDING[:2]:raise HTTPException(409,'仅补证中的替代争议可接着办')
    lock_deal(s,pre.deal_id)
    for r in s.scalars(select(DisputeEvidence).where(DisputeEvidence.dispute_id==id).order_by(DisputeEvidence.id)):
        x=lock(s,VaseSample,r.sample_id)
        if not x.prediction:enqueue(s,x)
    claim=advance(s,pre);audit(s,u,'补证接着办，沿用原争议案 #'+str(id));return pack(claim)
def decide(s,id,d,u):
    pre=s.get(Dispute,id)
    if not pre:raise HTTPException(404,'争议不存在')
    deal,lot=lock_deal(s,pre.deal_id);claim=lock(s,Dispute,id)
    if claim.state!='opened':raise HTTPException(409,'争议已复判或补证未就绪，不可复判')
    if RANK[d.grade]<RANK[deal.effective_grade]:raise HTTPException(409,'买方质量争议不可上调等级')
    days=claim.corrected_days if claim.mode=='proxy' else claim.sample_snapshot['observed_days'];observed='A'if days>=10 else 'B'if days>=7 else 'C'
    if RANK[d.grade]<RANK[observed]:raise HTTPException(409,'复判不可高于'+('修正估值'if claim.mode=='proxy' else '冻结留样天数')+'对应等级')
    claim.state='decided';claim.revised_grade=d.grade;claim.decision_note=d.note;deal.effective_grade=d.grade;deal.revision+=1
    for row in s.scalars(select(Settlement).where(Settlement.deal_id==deal.id,Settlement.state=='draft')):row.state='void'
    audit(s,u,'主管复判与结算修订 '+d.grade);return pack(claim)
def draft(s,id,u):
    deal,lot=lock_deal(s,id)
    if s.scalar(select(Dispute.id).where(Dispute.deal_id==id,Dispute.state.in_(PENDING))):raise HTTPException(409,'留样争议未复判，暂不结算')
    gross=(deal.unit_cents*deal.crates*WEIGHT[deal.effective_grade])//WEIGHT[deal.original_grade];commission=(gross*5)//100;target=gross-commission
    paid=s.scalar(select(func.coalesce(func.sum(Settlement.delta_cents),0)).where(Settlement.deal_id==id,Settlement.state=='paid'))
    row=Settlement(deal_id=id,revision=deal.revision,kind='initial'if not paid else'adjustment',gross_cents=gross,commission_cents=commission,target_cents=target,delta_cents=target-paid,snapshot={'deal':pack(deal),'grade_weights':WEIGHT,'commission_pct':5,'previous_paid_cents':paid})
    audit(s,u,'生成货款及补退差额');return add(s,row)
def pay(s,id,d,u):
    pre=s.get(Settlement,id)
    if not pre:raise HTTPException(404,'结算单不存在')
    deal,lot=lock_deal(s,pre.deal_id);row=lock(s,Settlement,id)
    if row.state!='draft' or row.revision!=deal.revision:raise HTTPException(409,'结算已支付、已作废或版本过期')
    if s.scalar(select(Dispute.id).where(Dispute.deal_id==deal.id,Dispute.state.in_(PENDING))):raise HTTPException(409,'争议未决不可支付')
    row.state='paid';row.receipt=d.receipt;audit(s,u,'登记实际货款/补退回单 '+d.receipt);return pack(row)

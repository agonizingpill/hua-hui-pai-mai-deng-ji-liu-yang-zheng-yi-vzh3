import os,time,json,logging
from pathlib import Path
import redis
from sqlalchemy import select
from .db import Session
from .models import VaseSample,Consignment,Dispute,DisputeEvidence
from .common import lock
from .grading import predict
from . import dispute_service
q=redis.Redis.from_url(os.environ['REDIS_URL'],decode_responses=True)
def process(item):
    with Session()as s:
        sample=lock(s,VaseSample,int(item))
        if sample.state!='queued':return
        sample.state='running';s.commit()
        try:
            result=predict(sample,s.get(Consignment,sample.consignment_id));sample.prediction=result;sample.state='ready';sample.error=None;Path(f'/artifacts/grade-{sample.id}.json').write_text(json.dumps(result,ensure_ascii=False));s.commit()
        except Exception as ex:sample.state='failed';sample.error=str(ex)[:450];s.commit();logging.exception('grade failed')
        try:
            for did in s.scalars(select(DisputeEvidence.dispute_id).where(DisputeEvidence.sample_id==sample.id)):
                claim=s.get(Dispute,did)
                if claim and claim.state=='estimating':dispute_service.advance(s,claim)
            s.commit()
        except Exception:s.rollback();logging.exception('dispute advance failed')
if __name__=='__main__':
    while True:
        Path('/tmp/floral-heartbeat').touch()
        try:
            item=q.blpop('floral-grade',timeout=2)
            if not item:continue
            process(item[1])
        except Exception:logging.exception('queue unavailable');time.sleep(1)

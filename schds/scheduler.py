import asyncio
from collections import defaultdict
from datetime import datetime
import logging
from typing import Dict
from schds.db import engine, init_db, get_session, SessionLocal
from sqlmodel import Session, select
from schds.models import JobInstanceModel, WorkerModel, JobModel

logger = logging.getLogger(__name__)


class SchdsScheduler:
    def __init__(self):
        self.worker_event_subscribers:Dict[str,list] = defaultdict(list)
        self.worker_events:Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        
    def start(self):
        pass

    def update_worker(self, name):
        with get_session() as session:
            statement = select(WorkerModel).where(WorkerModel.name == name)
            result = session.exec(statement)
            worker = result.first()
            if not worker:
                worker =WorkerModel(name=name, status='offline')
            session.add(worker)
            session.commit()
            session.refresh(worker)
            return worker
        
    def add_job(self, worker_name, job_name, cron):
        with get_session() as session:
            worker = session.exec(select(WorkerModel).where(WorkerModel.name == worker_name)).first()
            if worker is None:
                raise ValueError('worker not found.')

            statement = select(JobModel).where(JobModel.worker_id == worker.id, JobModel.name == job_name)
            result = session.exec(statement)
            job = result.first()
            if not job:
                job =JobModel(name=job_name, cron=cron, worker_id=worker.id, active=True)

            session.add(job)
            session.commit()
            logger.info('job added, %s-%s', worker_name, job_name)
            session.refresh(job)
            return job

    def subscribe_worker_events(self, worker_name, subscriber):
        self.worker_event_subscribers[worker_name].append(subscriber)

    def unsubscribe_worker_events(self, worker_name, subscriber):
        self.worker_event_subscribers[worker_name].remove(subscriber)

    def get_worker_event_queue(self, worker_name) -> asyncio.Queue:
        return self.worker_events[worker_name]
    
    def fire_job(self, worker_name, job_name) -> JobInstanceModel:
        with get_session() as session:
            worker = session.exec(select(WorkerModel).where(WorkerModel.name == worker_name)).first()
            if worker is None:
                raise ValueError('worker not found.')
            
            job = session.exec(select(JobModel).where(JobModel.name == job_name, JobModel.worker_id == worker.id)).first()
            if job is None:
                raise ValueError('job not found.')
            
            job_instance = JobInstanceModel(worker_id=worker.id, 
                                            job_id=job.id, 
                                            status='inqueue', 
                                            job_name=job.name, 
                                            start_time=datetime.now())
            session.add(job_instance)
            session.commit()
            session.refresh(job_instance)
            if worker_name in self.worker_events:
                self.worker_events[worker_name].put_nowait(job_instance)
            return job_instance

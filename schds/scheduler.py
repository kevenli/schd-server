import asyncio
from collections import defaultdict
from datetime import datetime
import logging
from typing import Dict

from sqlmodel import select
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from schds.db import get_session
from schds.models import JobInstanceModel, WorkerModel, JobModel


logger = logging.getLogger(__name__)


class WorkerAlreadyOnlineException(Exception):...


class SchdsScheduler:
    def __init__(self):
        self.worker_event_subscribers:Dict[str,list] = defaultdict(list)
        self._inner_scheduler = None
        
    def start(self):
        self._inner_scheduler = AsyncIOScheduler()
        with get_session() as session:
            for job in session.exec(select(JobModel).where(JobModel.active == True)).all():
                worker = session.exec(select(WorkerModel).where(WorkerModel.id == job.worker_id)).first()
                try:
                    cron_trigger = CronTrigger.from_crontab(job.cron)
                except ValueError:
                    # invalid cron
                    logger.info('invalid cron expression, %s, job.id: %d', job.cron, job.id)
                    continue

                self._inner_scheduler.add_job(self.fire_job, cron_trigger, kwargs={'worker_name': worker.name, 'job_name':job.name})

        self._inner_scheduler.start()

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
        if len(self.worker_event_subscribers[worker_name]) >= 1:
            # do not support more than one subscription at the time.
            raise WorkerAlreadyOnlineException()
        
        self.worker_event_subscribers[worker_name].append(subscriber)
        with get_session() as session:
            worker = session.exec(select(WorkerModel).where(WorkerModel.name == worker_name)).first()
            if worker is None:
                raise ValueError('worker not found.')
            
            inqueue_instances = session.exec(select(JobInstanceModel).where(JobInstanceModel.worker_id == worker.id,
                                                                            JobInstanceModel.status == 'INQUEUE')).all()
            for instance in inqueue_instances:
                job = session.exec(select(JobModel).where(JobModel.id == instance.job_id)).first()
                subscriber.send_job_instance_event(worker, job, instance)

    def unsubscribe_worker_events(self, worker_name, subscriber):
        self.worker_event_subscribers[worker_name].remove(subscriber)
    
    def fire_job(self, worker_name, job_name) -> JobInstanceModel:
        with get_session() as session:
            worker = session.exec(select(WorkerModel).where(WorkerModel.name == worker_name)).first()
            if worker is None:
                raise ValueError('worker not found.')
            
            job = session.exec(select(JobModel).where(JobModel.name == job_name, JobModel.worker_id == worker.id)).first()
            if job is None:
                raise ValueError('job not found.')
            
            inqueue_instances = session.exec(select(JobInstanceModel).where(JobInstanceModel.worker_id == worker.id,
                                                                             JobInstanceModel.job_id == job.id,
                                                                             JobInstanceModel.status == 'INQUEUE')).all()
            if len(inqueue_instances) >= 1:
                # by default, there will not be more than 1 instance for each job in the queue.
                logger.info('there are already %d instance of job %d, skip.', len(inqueue_instances), job.id)
                return inqueue_instances[-1]
            
            job_instance = JobInstanceModel(worker_id=worker.id,
                                            job_id=job.id,
                                            status='INQUEUE',
                                            job_name=job.name,
                                            start_time=datetime.now())
            session.add(job_instance)
            session.commit()
            session.refresh(job_instance)
            for subscriber in self.worker_event_subscribers[worker.name]:
                subscriber.send_job_instance_event(worker, job, job_instance)
            return job_instance

    def get_job_instance(self, worker_name, job_name, job_intance_id) -> JobInstanceModel:
        with get_session() as session:
            worker = session.exec(select(WorkerModel).where(WorkerModel.name == worker_name)).first()
            if worker is None:
                raise ValueError('worker not found.')
            
            job = session.exec(select(JobModel).where(JobModel.name == job_name, JobModel.worker_id == worker.id)).first()
            if job is None:
                raise ValueError('job not found.')
            
            job_instance = session.exec(select(JobInstanceModel).where(JobInstanceModel.id==job_intance_id,
                                                                       JobInstanceModel.job_id==job.id,
                                                                       JobInstanceModel.worker_id==worker.id)).first()
            return job_instance

    def start_job_instance(self, job_instance, new_status) -> JobInstanceModel:
        with get_session() as session:
            obj = session.exec(select(JobInstanceModel).where(JobInstanceModel.id==job_instance.id)).first()
            if not obj:
                raise ValueError('job_instance not found.')
            
            obj.status = new_status
            if new_status == 'RUNNING' and obj.actual_start_time is None:
                obj.actual_start_time = datetime.now()

            if (new_status == 'COMPLETED' or new_status == 'FAILED') and obj.stop_time is None:
                obj.stop_time = datetime.now()

            session.add(obj)
            session.commit()
            session.refresh(obj)
            return obj

    def complete_job_instance(self, job_instance, new_status, ret_code) -> JobInstanceModel:
        with get_session() as session:
            obj = session.exec(select(JobInstanceModel).where(JobInstanceModel.id==job_instance.id)).first()
            if not obj:
                raise ValueError('job_instance not found.')
            
            obj.status = new_status
            obj.ret_code = ret_code
            if (new_status == 'COMPLETED' or new_status == 'FAILED') and obj.stop_time is None:
                obj.stop_time = datetime.now()

            session.add(obj)
            session.commit()
            session.refresh(obj)
            return obj

import asyncio
from collections import defaultdict
from datetime import datetime
import logging
from typing import Dict, List

from sqlmodel import select
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from schds.db import get_session
from schds.models import JobInstanceModel, JobStatusTriggerModel, WorkerModel, JobModel
from pytz import timezone as ZoneInfo


logger = logging.getLogger(__name__)


class WorkerAlreadyOnlineException(Exception):...


class JobResultTrigger:
    on_job_id: int # observer on which job
    # on_job: JobModel # observer on which job
    on_job_result_status: str # on which status this can trigger  [ANY] | COMPLETED | FAILED
    fire_job:JobModel
    fire_job_id: int


class SchdsScheduler:
    def __init__(self):
        self.worker_event_subscribers:Dict[str,list] = defaultdict(list)
        self._inner_scheduler = AsyncIOScheduler()
        self.job_result_triggers:Dict[int, List[JobResultTrigger]] = defaultdict(list)
        self._jobs:Dict[int, JobModel] = dict()

    def init(self):
         with get_session() as session:
             for job in session.exec(select(JobModel).where(JobModel.active == True)).all():
                 self._jobs[job.id] = job
                 self._schedule_job(job)
 
             for job_status_trigger in session.exec(select(JobStatusTriggerModel)).all():
                 self.place_job_result_trigger(job_status_trigger)
        
    def start(self):
        # to know job next_run_time, it has to start scheduler first.
        self._inner_scheduler.start()
        # with get_session() as session:
        #     for job in session.exec(select(JobModel).where(JobModel.active == True)).all():
        #         self._schedule_job(job)

    def _schedule_job(self, job:JobModel):
        job_id = str(job.id)
        exist_job = self._inner_scheduler.get_job(job_id=job_id)
        if exist_job:
            self._inner_scheduler.remove_job(job_id=job_id)

        if not job.cron:
            return
        
        try:
            job_timezone = None
            if job.timezone:
                job_timezone = ZoneInfo(job.timezone)
            cron_trigger = CronTrigger.from_crontab(job.cron, timezone=job_timezone)
        except ValueError:
            # invalid cron
            logger.error('invalid cron expression, %s, job.id: %d', job.cron, job.id)
            return

        job_obj = self._inner_scheduler.add_job(self.fire_job2, cron_trigger, kwargs={'job':job}, id=job_id)
        
        try:
            next_run_time = job_obj.next_run_time
        except AttributeError:
            # cannot be `next_run_time` if scheduler is not started.
            next_run_time = None
        logger.info('job added %s, next wakeup time %s', job.id, next_run_time)

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
        
    def get_all_workers(self):
        with get_session() as session:
            return session.exec(select(WorkerModel)).all()
        
    def find_worker(self, name):
        with get_session() as session:
            statement = select(WorkerModel).where(WorkerModel.name == name)
            result = session.exec(statement)
            worker = result.first()
            return worker
        
    def add_job(self, worker_name, job_name, cron, timezone=None):
        with get_session() as session:
            worker = session.exec(select(WorkerModel).where(WorkerModel.name == worker_name)).first()
            if worker is None:
                raise ValueError('worker not found.')
            
            try:
                job_timezone = None
                if timezone:
                    job_timezone = ZoneInfo(timezone)
                if cron:
                    _ = CronTrigger.from_crontab(cron, job_timezone) # try parse cron expression before saving.
            except ValueError:
                # invalid cron
                logger.info('invalid cron expression, %s', cron)
                raise

            statement = select(JobModel).where(JobModel.worker_id == worker.id, JobModel.name == job_name)
            result = session.exec(statement)
            job = result.first()
            if not job:
                job =JobModel(name=job_name, cron=cron, worker_id=worker.id, active=True, timezone=timezone)

            job.cron = cron
            job.timezone = timezone
            job.active = True
            session.add(job)
            session.commit()
            session.refresh(job)
            self._jobs[job.id] = job
            logger.info('job added, %s-%s', worker_name, job_name)

            self._schedule_job(job)
            return job
        
    def find_job(self, worker_id, job_name):
        with get_session() as session:
            statement = select(JobModel).where(JobModel.worker_id == worker_id, JobModel.name == job_name)
            result = session.exec(statement)
            job = result.first()
            return job
    
    def get_job(self, job_id:int) -> JobModel:
        return self._jobs.get(job_id)

    def get_all_jobs(self):
        with get_session() as session:
            return session.exec(select(JobModel).where(JobModel.active == True)).all()
        

    def subscribe_worker_events(self, worker_name, subscriber):
        if len(self.worker_event_subscribers[worker_name]) >= 1:
            # do not support more than one subscription at the time.
            raise WorkerAlreadyOnlineException()
        
        self.worker_event_subscribers[worker_name].append(subscriber)
        with get_session() as session:
            worker = session.exec(select(WorkerModel).where(WorkerModel.name == worker_name)).first()
            if worker is None:
                raise ValueError('worker not found.')
            
            worker.status = 'online'
            session.add(worker)
            session.commit()
            
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

    def fire_job2(self, job:JobModel) -> JobInstanceModel:
        logger.info('fire job %d-%s %s', job.id, job.worker_id, job.name)
        with get_session() as session:
            worker = session.exec(select(WorkerModel).where(WorkerModel.id == int(job.worker_id))).first()
            
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

            for trigger in self.job_result_triggers[job_instance.job_id]:
                if trigger.on_job_result_status == '[ANY]' or trigger.on_job_result_status == new_status:
                    self.fire_job2(trigger.fire_job)

            return obj

    def add_job_result_trigger(self, on_job:JobModel, fire_job:JobModel, on_job_result_status:str):
        if not on_job_result_status in ['[ANY]', 'COMPLETED', 'FAILED']:
            raise ValueError('invalid status')

        with get_session() as session:
            exist_trigger = session.exec(select(JobStatusTriggerModel).where(
                JobStatusTriggerModel.on_job_id == on_job.id,
                JobStatusTriggerModel.fire_job_id == fire_job.id,
                JobStatusTriggerModel.on_job_status == on_job_result_status
            )).first()

            if exist_trigger:
                return exist_trigger

            trigger_model = JobStatusTriggerModel(on_job_id=on_job.id, fire_job_id=fire_job.id, on_job_status=on_job_result_status)
            session.add(trigger_model)
            session.commit()
            session.refresh(trigger_model)

            self.place_job_result_trigger(trigger_model)

            return trigger_model
        # trigger = JobResultTrigger()
        # # trigger.on_job = on_job
        # trigger.on_job_id = on_job.id
        # trigger.fire_job_id = fire_job.id
        # trigger.fire_job = fire_job
        # trigger.on_job_result_status = on_job_result_status
        # existing_triggers = filter(lambda t: t.fire_job.id == fire_job.id, self.job_result_triggers[on_job.id])
        # for existing_trigger in existing_triggers:
        #     self.job_result_triggers[on_job.id].remove(existing_trigger)

        # self.job_result_triggers[on_job.id].append(trigger)

    def place_job_result_trigger(self, trigger_model: JobStatusTriggerModel):
        trigger = JobResultTrigger()
        on_job = self.get_job(trigger_model.on_job_id)
        fire_job = self.get_job(trigger_model.fire_job_id)
        # trigger.on_job = on_job
        trigger.on_job_id = on_job.id
        trigger.fire_job_id = fire_job.id
        trigger.fire_job = fire_job
        trigger.on_job_result_status = trigger_model.on_job_status
        existing_triggers = filter(lambda t: t.fire_job.id == fire_job.id, self.job_result_triggers[on_job.id])
        for existing_trigger in existing_triggers:
            self.job_result_triggers[on_job.id].remove(existing_trigger)

        self.job_result_triggers[on_job.id].append(trigger)

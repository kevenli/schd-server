from schds.db import engine, init_db, get_session, SessionLocal
from sqlmodel import Session, select
from schds.models import WorkerModel, JobModel


class SchdsScheduler:
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

            statement = select(JobModel).where(JobModel.worker_id == worker.id)
            result = session.exec(statement)
            job = result.first()
            if not job:
                job =JobModel(name=job_name, cron=cron, worker_id=worker.id, active=True)

            session.add(job)
            session.commit()
            session.refresh(job)
            return job

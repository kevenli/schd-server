from datetime import datetime
from sqlmodel import Relationship, SQLModel, Field
from typing import Optional

class JobModel(SQLModel, table=True):
    __tablename__ = 'jobs'
    
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    active: bool
    cron: Optional[str]
    worker_id: Optional[str]
    timezone: Optional[str]


class WorkerModel(SQLModel, table=True):
    __tablename__ = 'workers'

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    status: str


class JobInstanceModel(SQLModel, table=True):
    __tablename__ = 'job_instances'

    id: Optional[int] = Field(default=None, primary_key=True)
    worker_id: int
    job_id: int
    job_name: str
    status: str
    start_time: datetime
    actual_start_time: Optional[datetime]
    stop_time: Optional[datetime]
    ret_code: Optional[int]


class JobStatusTriggerModel(SQLModel, table=True):
    __tablename__ = 'jobstatus_triggers'
    id: Optional[int] = Field(default=None, primary_key=True)
    on_job_id: int = Field(foreign_key="jobs.id")
    on_job_status: str
    fire_job_id: int = Field(foreign_key="jobs.id")
    on_job: JobModel = Relationship(sa_relationship_kwargs={"lazy":"selectin", "foreign_keys":"JobStatusTriggerModel.on_job_id"})
    fire_job: JobModel = Relationship(sa_relationship_kwargs={"foreign_keys":"JobStatusTriggerModel.fire_job_id"})


def create_tables(engine):
    SQLModel.metadata.create_all(engine)

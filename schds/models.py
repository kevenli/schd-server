from datetime import datetime
from sqlmodel import SQLModel, Field
from typing import Optional

class JobModel(SQLModel, table=True):
    __tablename__ = 'jobs'
    
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    active: bool
    cron: Optional[str]
    worker_id: Optional[str]


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

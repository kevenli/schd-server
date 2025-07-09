import os
from typing import List, Union
import unittest
from schds.db import upgrade_database, init_db, create_tables
import schds.db as db
from schds.scheduler import SchdsScheduler
from schds.models import WorkerModel, JobModel, JobInstanceModel


class SchdsSchedulerJobResultTriggerTest(unittest.TestCase):
    class WorkerObserver:
        def __init__(self):
            self.events: List[Union[WorkerModel, JobModel, JobInstanceModel]] = list()

        def send_job_instance_event(self, worker, job, job_instance):
            self.events.append((worker, job, job_instance))

    def setUp(self):
        if os.path.exists('tests/schds_test.db'):
            os.remove('tests/schds_test.db')

        # init_db('sqlite:///tests/schds_test.db', auto_upgrade=False)
        # create_tables(db.engine)
        init_db('sqlite:///tests/schds_test.db', auto_upgrade=True)

    def test_add_job_result_trigger(self):
        target = SchdsScheduler()
        worker_name = 'worker'
        worker = target.update_worker(worker_name)
        observer = SchdsSchedulerJobResultTriggerTest.WorkerObserver()
        target.subscribe_worker_events(worker_name, observer)
        self.assertEqual(0, len(observer.events))
        task1 = target.add_job(worker_name, 'task1', '')
        task2 = target.add_job(worker_name, 'task2', '')
        trigger = target.add_job_result_trigger(task1, task2, 'COMPLETED')

        for _ in range(1):
            # first round, job complete with FAILED
            task1_instance = target.fire_job2(task1)
            self.assertEqual(1, len(observer.events))
            self.assertEqual(task1.id, observer.events[0][1].id)
            observer.events.pop(0)
            
            # FAILED does not match trigger, no new job instance created.
            target.complete_job_instance(task1_instance, 'FAILED', 0)
            self.assertEqual(0, len(observer.events))

        for _ in range(1):
            # simulate task1 been started.
            task1_instance = target.fire_job2(task1)
            self.assertEqual(1, len(observer.events))
            self.assertEqual(task1.id, observer.events[0][1].id)
            observer.events.pop(0)
            
            target.complete_job_instance(task1_instance, 'COMPLETED', 0)
            # after task1 COMPLETED, a new instace of task2 should have been established.
            self.assertEqual(1, len(observer.events))
            self.assertEqual(task2.id, observer.events[0][1].id)
            observer.events.pop(0)
            target.unsubscribe_worker_events(worker_name, observer)

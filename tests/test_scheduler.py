import os
from typing import List, Union
import unittest
from schds.db import upgrade_database
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

        upgrade_database('tests/schds_test.db')

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

        task1_instance = target.fire_job2(task1)
        
        self.assertEqual(1, len(observer.events))
        self.assertEqual(task1.id, observer.events[0][1].id)
        observer.events.pop(0)

        target.complete_job_instance(task1_instance, 'COMPLETED', 0)
        self.assertEqual(1, len(observer.events))
        self.assertEqual(task2.id, observer.events[0][1].id)

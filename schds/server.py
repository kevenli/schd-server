import asyncio
import logging
import os
import signal
import sys
import tornado
import tornado.web
import tornado.websocket

from schds.config import read_config, SchdsConfig
from schds.models import JobInstanceModel
from schds.scheduler import SchdsScheduler, WorkerAlreadyOnlineException
from schds.db import init_db

logger = logging.getLogger(__name__)

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")


class WebViewBase(tornado.web.RequestHandler):
    @property
    def scheduler(self) -> SchdsScheduler:
        return self.settings['scheduler']


class WorkersView(WebViewBase):
    """
    /workers
    """
    def get(self):
        workers = self.scheduler.get_all_workers()
        self.render("workers.html", workers=workers)


class AllJobsView(WebViewBase):
    def get(self):
        jobs = self.scheduler.get_all_jobs()
        job_trigger_counts = {}
        job_last_instances = {}
        for job in jobs:
            job_trigger_counts[job.id] = len(self.scheduler.get_job_triggers(job.id))
            job_last_instances[job.id] = self.scheduler.get_job_last_instance(job.id)
        self.render('jobs.html',
                    jobs=jobs,
                    job_trigger_counts=job_trigger_counts,
                    job_last_instances=job_last_instances)
        

class JobInfoView(WebViewBase):
    def get(self, job_id):
        job_id = int(job_id)
        job = self.scheduler.get_job(job_id)
        job_instances = self.scheduler.get_job_recent_instances(job_id)
        self.render('jobinfo.html',
                    job=job,
                    job_instances=job_instances)
        

class DeleteJobView(WebViewBase):
    def post(self, job_id):
        job_id = int(job_id)
        job = self.scheduler.get_job(job_id)
        self.scheduler.delete_job(job)
        

class JobTriggersView(WebViewBase):
    def get(self, job_id):
        job_id = int(job_id)
        job = self.scheduler.get_job(job_id)
        if not job:
            self.set_status(404, 'job not found.')
            return self.write_error(404)
        
        job_triggers = self.scheduler.get_job_triggers(job_id)
        self.render('jobtriggers.html',
                    job=job,
                    job_triggers=job_triggers)
        
    def post(self, job_id):
        job_id = int(job_id)
        job = self.scheduler.get_job(job_id)
        if not job:
            self.set_status(404, 'job not found.')
            return self.write_error(404)
        
        on_job_id = int(self.get_body_argument('on_job_id'))
        on_job_status = self.get_body_argument('on_job_status')
        on_job = self.scheduler.get_job(on_job_id)
        
        trigger = self.scheduler.add_job_result_trigger(on_job=on_job, fire_job=job, on_job_result_status=on_job_status)
        

class JobTriggerDeleteView(WebViewBase):
    def post(self, job_id):
        job_id = int(job_id)
        job = self.scheduler.get_job(job_id)
        if not job:
            self.set_status(404, 'job not found.')
            return self.write_error(404)
        
        trigger_id = int(self.get_argument('trigger_id'))
        
        self.scheduler.delete_job_trigger(job, trigger_id)


class FireJobView(WebViewBase):
    def post(self, job_id):
        job_id = int(job_id)
        job = self.scheduler.get_job(job_id)
        if not job:
            self.set_status(404, 'job not found')
            return self.write_error(404)
        
        self.scheduler.fire_job2(job)


class JobInstanceLogView(WebViewBase):
    def get(self, job_id, job_instance_id):
        job_dir = f'joblog/{job_instance_id}'

        job_filepath = os.path.join(job_dir, 'output.txt')
        with open(job_filepath, "r") as f:
            self.write(f.read())


class JSONHandler(tornado.web.RequestHandler):
    def _return_response(self, request, message_to_be_returned: dict, status_code):
        """
        Returns formatted response back to client
        """
        try:
            request.set_header("Content-Type", "application/json; charset=UTF-8")
            request.set_status(status_code)

            #If dictionary is not empty then write the dictionary directly into
            if(bool(message_to_be_returned)):
                request.write(message_to_be_returned)

            request.finish()
        except Exception:
            raise


class RegisterWorkerHandler(JSONHandler):
    def put(self, worker_name):
        scheduler:"SchdsScheduler" = self.settings['scheduler']
        new_worker = scheduler.update_worker(worker_name)
        self._return_response(self, {'name':new_worker.name}, 200)


class RegisterJobHandler(JSONHandler):
    """
    PUT /api/workers/{worker_name}/jobs/{job_name}
    """
    def put(self, worker_name, job_name):
        request_payload:dict = tornado.escape.json_decode(self.request.body)
        cron = request_payload.get('cron')
        cron_timezone = request_payload.get('timezone')
        scheduler:"SchdsScheduler" = self.settings['scheduler']
        new_job = scheduler.add_job(worker_name, job_name, cron, timezone=cron_timezone)
        return self._return_response(self, {
            'name': new_job.name,
            'cron': new_job.cron,
            'timezone': new_job.timezone,
            'active': new_job.active,
        }, 200)
    
    def get(self, worker_name, job_name):
        scheduler:"SchdsScheduler" = self.settings['scheduler']
        worker = scheduler.find_worker(worker_name)
        if not worker:
            return self._return_response(self, {'error': 'worker not found'}, 404)
        
        job = scheduler.find_job(worker.id, job_name)
        if not job:
            return self._return_response(self, {'error': 'job not found'}, 404)
        
        return self._return_response(self, {
            'name': job.name,
            'cron': job.cron,
            'timezone': job.timezone,
            'active': job.active,
        }, 200)


class WorkerEventsHandler(tornado.web.RequestHandler):
    async def prepare(self):
        worker_name = self.worker_name = self.path_kwargs["worker_name"]
        scheduler:"SchdsScheduler" = self.settings['scheduler']
        self.queue = asyncio.Queue()
        client_version_header = self.request.headers.get("X-SchdClient", 'schd_0.1.2')
        client_version_number = client_version_header.split('_', 2)[1]
        self.client_version_major = int(client_version_number.split('.')[0])
        self.client_version_minor = int(client_version_number.split('.')[1])
        client_ip = self.request.remote_ip
        logger.info('WorkerEventsHandler worker_name: %s, client_version: %s, client_ip: %s', worker_name, client_version_header, client_ip)

        try:
            scheduler.subscribe_worker_events(worker_name, self)
            self.set_header("Content-Type", "text/event-stream")
            self.set_header("Cache-Control", "no-cache")
            self.set_header("Connection", "keep-alive")
            self.flush()
            logger.info(f"SSE connection opened for {self.worker_name}")
            await self.send_events()
        except WorkerAlreadyOnlineException:
            self.set_status(409, 'worker already online')
            return self.write_error(409)
        
    def send_job_instance_event(self, worker, job, job_instance):
        logger.info('send_job_instance_event, %s, %s, %s', worker, job, job_instance)
        self.queue.put_nowait(job_instance)

    async def send_events(self):
        self.running = True
        while self.running:
            try:
                # to prevent trying from a died connection forever, get with a TIMEOUT
                event = await asyncio.wait_for(self.queue.get(), timeout=10)
            except asyncio.TimeoutError:
                if self.client_version_major <= 0 and self.client_version_minor <= 1:
                    # do not send heartbeat if client version <= 0.1
                    continue
                
                self.write({
                    'event_type': 'heartbeat',
                    'data': {}
                })
                self.write('\n')
                self.flush()
                continue

            # event = await self.queue.get()
            logger.info('got event %s, %s', self.worker_name, event)
            if isinstance(event, JobInstanceModel):
                self.write({
                    'event_type': 'NewJobInstance',
                    'data': {
                        'id': event.id,
                        'job_name': event.job_name,
                        'start_time': int(event.start_time.timestamp()*1000),
                    }
                })
                self.write('\n')
                self.flush()
            else:
                logger.error('unknown event type')

    def on_connection_close(self):
        logger.info(f"Connection closed for worker {self.worker_name}")
        self.running = False
        scheduler:"SchdsScheduler" = self.settings['scheduler']
        scheduler.unsubscribe_worker_events(self.worker_name, self)


class FireJobHandler(JSONHandler):
    """
    /api/workers/xxx/jobs/yyy:fire
    """
    def post(self, worker_name, job_name):
        scheduler:"SchdsScheduler" = self.settings['scheduler']
        new_job_instance = scheduler.fire_job(worker_name, job_name)
        self._return_response(self, {
            'id': new_job_instance.id,
        }, 200)


class JobTriggersHandler(JSONHandler):
    """
    /api/workers/{worker_name}/jobs/{job_name}/triggers
    """
    def post(self, worker_name, job_name):
        request_payload:dict = tornado.escape.json_decode(self.request.body)
        scheduler:"SchdsScheduler" = self.settings['scheduler']
        worker = scheduler.find_worker(worker_name)
        if not worker:
            return self._return_response(self, {'error': 'worker not found'}, 404)
        
        target_job = scheduler.find_job(worker.id, job_name)
        if not target_job:
            return self._return_response(self, {'error': 'job not found'}, 404)
        
        on_job_name = request_payload['on_job_name']
        on_job_worker_name = request_payload.get('on_worker_name', worker_name)
        on_job_status = request_payload['on_job_status']

        on_worker = scheduler.find_worker(on_job_worker_name)
        if not on_worker:
            return self._return_response(self, {'error': 'invalid on_worker_name'}, 404)
        
        on_job = scheduler.find_job(on_worker.id, on_job_name)
        if not on_job:
            return self._return_response(self, {'error': 'invalid on_job_name'}, 404)
        
        trigger = scheduler.add_job_result_trigger(on_job, target_job, on_job_status)
        self._return_response(self, {
            # 'trigger_id': trigger.id,
            'target_job_name': target_job.name,
        }, 200)


class UpdateJobHandler(JSONHandler):
    def put(self, worker_name, job_name, job_instance_id):
        scheduler:"SchdsScheduler" = self.settings['scheduler']
        job_instance = scheduler.get_job_instance(worker_name, job_name, job_instance_id)

        request_payload:dict = tornado.escape.json_decode(self.request.body)
        status = request_payload['status']
        if status == 'RUNNING':
            new_instance = scheduler.start_job_instance(job_instance, new_status=status)
            self._return_response(self, {'id': new_instance.id, 'status': new_instance.status}, 200)
        elif status == 'COMPLETED':
            new_instance = scheduler.complete_job_instance(job_instance, status, request_payload['ret_code'])
            self._return_response(self, {'id': new_instance.id, 'status': new_instance.status}, 200)
        else:
            raise ValueError('invalid status')
        

class UpdateInstanceLogHandler(tornado.web.RequestHandler):
    def put(self, worker_name, job_name, job_instance_id):
        scheduler:"SchdsScheduler" = self.settings['scheduler']
        files = self.request.files.get("logfile", [])

        job_instance = scheduler.get_job_instance(worker_name, job_name, job_instance_id)
        if job_instance.status != 'RUNNING':
            raise ValueError('job instace is not running, cannot commit log file.')
        
        assert len(files) == 1

        for fileinfo in files:
            filename = fileinfo["filename"]
            body = fileinfo["body"]

            job_dir = f'joblog/{job_instance_id}'
            if not os.path.exists(job_dir):
                os.makedirs(job_dir)

            job_filepath = os.path.join(job_dir, 'output.txt')
            with open(job_filepath, "wb") as f:
                f.write(body)

            self.write(f"Saved {filename} to {job_filepath}\n")


class WorkerWSEventsHandler(tornado.websocket.WebSocketHandler):
    def initialize(self):
        self.worker_name = None

    def check_origin(self, origin):
        # Allow CORS (optional, useful during dev)
        return True

    def open(self, worker_name):
        self.worker_name = worker_name
        logger.info(f"WebSocket opened for worker: {worker_name}")
        # self.write_message(f"Connected to {worker_name}")

    def on_message(self, message):
        logger.info(f"Received message from {self.worker_name}: {message}")
        # Echo or process the message
        response = f"{self.worker_name} received: {message}"
        # self.write_message(response)

    def on_close(self):
        logger.info(f"WebSocket closed for worker: {self.worker_name}")


def make_app(scheduler):
    worker_name_ptrn = r'(?P<worker_name>[a-zA-Z][a-zA-Z0-9_]{0,35})'
    job_name_ptrn = r'(?P<job_name>[a-zA-Z][a-zA-Z0-9_]{0,35})'
    job_instance_ptrn = r'(?P<job_instance_id>\d+)'

    template_dir = os.path.join(os.path.dirname(__file__), "web_templates")

    return tornado.web.Application([
        (r"/", MainHandler),
        (r'/workers', WorkersView),
        (r'/jobs', AllJobsView),
        (r'/jobs/(\d+)', JobInfoView),
        (r'/jobs/(\d+)/delete', DeleteJobView),
        (r'/jobs/(\d+)/triggers', JobTriggersView),
        (r'/jobs/(\d+)/triggers/delete', JobTriggerDeleteView),
        (r'/jobs/(\d+)/instances/(\d+)/log', JobInstanceLogView),
        (r'/jobs/(\d+)/fire', FireJobView),
        (f"/api/workers/{worker_name_ptrn}", RegisterWorkerHandler),
        (f"/api/workers/{worker_name_ptrn}/jobs/{job_name_ptrn}", RegisterJobHandler),
        (f"/api/workers/{worker_name_ptrn}/jobs/{job_name_ptrn}:fire", FireJobHandler),
        (f"/api/workers/{worker_name_ptrn}/jobs/{job_name_ptrn}/triggers", JobTriggersHandler),
        (f"/api/workers/{worker_name_ptrn}/jobs/{job_name_ptrn}/{job_instance_ptrn}", UpdateJobHandler),
        (f"/api/workers/{worker_name_ptrn}/jobs/{job_name_ptrn}/{job_instance_ptrn}/log", UpdateInstanceLogHandler),
        (f"/api/workers/{worker_name_ptrn}/eventstream", WorkerEventsHandler),
        (f"/wsapi/workers/{worker_name_ptrn}/events", WorkerWSEventsHandler), 
    ],
    template_path=template_dir,
    scheduler=scheduler)


class SchdServer:
    def __init__(self, config):
        self._config = config

        if not os.path.exists('data'):
            os.makedirs('data')
        init_db(config.db_url)
        # upgrade_database(config.db_url)
        self._scheduler = SchdsScheduler()
        self._scheduler.init()
        self._scheduler.start()
        self._app = make_app(scheduler=self._scheduler)
        self._http_server = None
        
    async def run(self):
        http_port=8899
        self._http_server = server = self._app.listen(http_port, address="0.0.0.0")
        logger.info(f"Server started on http://localhost:{http_port}")

        stop_event = asyncio.Event()

        def handle_signal():
            print("Received shutdown signal")
            stop_event.set()

        # for sig in (signal.SIGINT, signal.SIGTERM):
        #     signal.signal(signal.SIGINT, handle_signal)
        #     signal.signal(signal.SIGTERM, handle_signal)
        loop = asyncio.get_event_loop()
        try:
            loop.add_signal_handler(signal.SIGINT, handle_signal)
            loop.add_signal_handler(signal.SIGTERM, handle_signal)
        except NotImplementedError:
            # On Windows, fallback: handle KeyboardInterrupt manually
            logger.warning('loop.add_signal_handler not implemented')

        try:
            await stop_event.wait()
        except KeyboardInterrupt:
            logger.info("Received KeyboardInterrupt, exiting.")
        server.stop()
        await asyncio.sleep(1)  # Graceful shutdown delay


async def main():
    server_config = read_config()
    logging.basicConfig(level=logging.DEBUG)
    server = SchdServer(server_config)
    try:
        await server.run()
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, shutting down.")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            logger.info("Received KeyboardInterrupt, exiting.")
    else:
        asyncio.run(main())

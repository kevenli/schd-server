import asyncio
import json
import logging
import signal
import sys
import tornado
import tornado.web
import tornado.websocket

from schds.config import read_config
from schds.models import JobInstanceModel
from schds.scheduler import SchdsScheduler
from schds.db import init_db

logger = logging.getLogger(__name__)

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")


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
    def post(self):
        request_payload = tornado.escape.json_decode(self.request.body)
        name = request_payload['name']
        scheduler:"SchdsScheduler" = self.settings['scheduler']
        new_worker = scheduler.update_worker(name)
        self._return_response(self, {'id': new_worker.id,'name':new_worker.name}, 200)


class RegisterJobHandler(JSONHandler):
    """
    PUT /api/workers/{worker_name}/jobs/{job_name}
    """
    def put(self, worker_name, job_name):
        request_payload:dict = tornado.escape.json_decode(self.request.body)
        cron = request_payload.get('cron')
        scheduler:"SchdsScheduler" = self.settings['scheduler']
        new_job = scheduler.add_job(worker_name, job_name, cron)
        return self._return_response(self, {
            'id': new_job.id, 
            'name': new_job.name,
        }, 200)

class WorkerEventsHandler(tornado.web.RequestHandler):
    async def prepare(self):
        worker_name = self.worker_name = self.path_kwargs["worker_name"]
        self.set_header("Content-Type", "text/event-stream")
        self.set_header("Cache-Control", "no-cache")
        self.set_header("Connection", "keep-alive")
        self.flush()
        logger.info(f"SSE connection opened for {self.worker_name}")
        scheduler:"SchdsScheduler" = self.settings['scheduler']
        scheduler.subscribe_worker_events(worker_name, self)

        queue = scheduler.get_worker_event_queue(worker_name)
        self.running = True
        while self.running:
            event = await queue.get()
            logger.info('got event %s, %s', worker_name, event)
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

        # self.running = True
        # self.send_events()

    def send_job_instance_event(self, worker, job, job_instance):
        logger.info('send_job_instance_event, %s, %s, %s', worker, job, job_instance)

    async def send_events(self):
        count = 0
        while self.running:
            await tornado.gen.sleep(1)
            data = f"data: Event {count} from {self.worker_name}\n\n"
            try:
                self.write(data)
                await self.flush()
                count += 1
            except tornado.iostream.StreamClosedError:
                logger.info(f"Client for {self.worker_name} disconnected")
                self.running = False

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
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/api/workers", RegisterWorkerHandler),
        (r"/api/workers/(?P<worker_name>[a-zA-Z][a-zA-Z0-9]{0,35})/jobs/(?P<job_name>[a-zA-Z][a-zA-Z0-9]{0,35})", RegisterJobHandler),
        (r"/api/workers/(?P<worker_name>[a-zA-Z][a-zA-Z0-9]{0,35})/jobs/(?P<job_name>[a-zA-Z][a-zA-Z0-9]{0,35}):fire", FireJobHandler),
        (r"/api/workers/(?P<worker_name>[a-zA-Z][a-zA-Z0-9]{0,35})/eventstream", WorkerEventsHandler),
        (r"/wsapi/workers/(?P<worker_name>[a-zA-Z][a-zA-Z0-9]{0,35})/events", WorkerWSEventsHandler), 
    ], scheduler=scheduler)


class SchdServer:
    def __init__(self, config):
        self._config = config
        init_db(config.db_url)
        self._scheduler = SchdsScheduler()
        self._scheduler.start()
        self._app = make_app(scheduler=self._scheduler)
        self._http_server = None
        
    async def run(self):
        http_port=8899
        self._http_server = server = self._app.listen(http_port)
        print(f"Server started on http://localhost:{http_port}")

        stop_event = asyncio.Event()

        def handle_signal(signum, frame):
            print(f"Received signal {signum}, shutting down...")
            stop_event.set()

        # for sig in (signal.SIGINT, signal.SIGTERM):
        #     signal.signal(signal.SIGINT, handle_signal)
        #     signal.signal(signal.SIGTERM, handle_signal)
        loop = asyncio.get_event_loop()
        try:
            loop.add_signal_handler(signal.SIGINT, handle_signal)
        except NotImplementedError:
            # On Windows, fallback: handle KeyboardInterrupt manually
            pass

        await stop_event.wait()
        server.stop()
        await asyncio.sleep(1)  # Graceful shutdown delay


async def main():
    server_config = read_config()
    logging.basicConfig(level=logging.INFO)
    server = SchdServer(server_config)
    await server.run()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())

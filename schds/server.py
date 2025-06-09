import asyncio
import json
import signal
import tornado

from schds.config import read_config
from schds.scheduler import SchdsScheduler
from schds.db import init_db

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


def make_app(scheduler):
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/api/workers", RegisterWorkerHandler),
        (r"/api/workers/(?P<worker_name>[a-zA-Z][a-zA-Z0-9]{2,35})/jobs/(?P<job_name>[a-zA-Z][a-zA-Z0-9]{2,35})", RegisterJobHandler),
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

        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(signal.SIGINT, handle_signal)
            signal.signal(signal.SIGTERM, handle_signal)

        await stop_event.wait()
        server.stop()
        await asyncio.sleep(1)  # Graceful shutdown delay


async def main():
    server_config = read_config()
    server = SchdServer(server_config)
    await server.run()

if __name__ == "__main__":
    asyncio.run(main())

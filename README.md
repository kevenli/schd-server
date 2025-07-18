# schd-server
docker run -v ./conf:/app/conf -v ./data:/app/data -p 8899:8899 kevenli/schd-server

To let schd run in remote mode, add schd configuration 
env
``` bash
SCHD_SCHEDULER_CLS=RemoteScheduler
SCHD_SCHEDULER_REMOTE_HOST=http://localhost:8899
SCHD_WORKER_NAME=local
```

schd.yaml
``` yaml
scheduler_cls: RemoteScheduler
scheduler_remote_host: http://localhost:8899
worker_name: local
```
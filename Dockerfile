# build
FROM python:3.9-slim-bullseye AS builder

RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    default-libmysqlclient-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /venv
ENV PATH="/venv/bin:$PATH"

WORKDIR /app_src
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY schds /app_src/schds
ADD setup.py /app_src/
# RUN python setup.py install
RUN pip install .

# run 
FROM python:3.9-slim-bullseye
RUN apt-get update && apt-get install -y \
    default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*
EXPOSE 8899
WORKDIR /app
COPY --from=builder /venv /venv
ENV PATH="/venv/bin:/root/.local/bin:$PATH"
CMD python -m schds.server

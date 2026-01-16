FROM python:3.12-slim-bookworm

ARG WORK_DIR=/search_api

WORKDIR $WORK_DIR

COPY src/requirements.txt $WORK_DIR/requirements.txt

RUN pip install --no-cache-dir -r $WORK_DIR/requirements.txt

# copy project
COPY ./src $WORK_DIR/
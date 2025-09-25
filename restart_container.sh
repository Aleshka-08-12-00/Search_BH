#!/bin/bash
export PATH=$PATH:/usr/local/bin
cd /home/ubuntu/search_test/search_api
docker-compose down
docker-compose build
docker-compose up -d

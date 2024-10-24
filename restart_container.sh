#!/bin/bash

cd
cd search_test/search_api
docker-compose down
docker-compose build
docker-compose up -d

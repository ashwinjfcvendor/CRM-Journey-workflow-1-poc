#!/bin/bash

docker compose down -v

cd clickhouse

docker compose down -v 

echo -e "\n Setup destroy completed"
#!/bin/bash

# Check if clickhouse directory exists
if [ ! -d "clickhouse" ]; then
  echo "Error: Directory 'clickhouse' not found."
  exit 1
fi

cd clickhouse

docker compose build

docker compose up -d

echo -e "\n starting up..."
for ((i=10; i>0; i--)); do
  sleep 1
  printf "\r"
done
echo

./setup_flink_clickhouse.sh

cd ..

docker compose up -d

echo -e "\n starting up temporal..."
for ((i=10; i>0; i--)); do
  sleep 1
  printf "\r"
done
echo

./create_user_state_db_table_in_postgres.sh


echo -e "\n Setup Completed"
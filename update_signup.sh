#!/bin/bash

# Check if client_id is provided
# if [ -z "$1" ]; then
#   echo "Usage: $0 <client_id>"
#   exit 1
# fi

CLIENT_ID="1000003"
DOCKER_CONTAINER_NAME="temporal-postgresql"
DB_NAME="userstate"
TABLE_NAME="user_state"
DB_USER="temporal"

# SQL query to update the table
SQL_QUERY="UPDATE ${TABLE_NAME} SET is_signed_up = TRUE, signup_time = TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') WHERE client_id = '${CLIENT_ID}';"

# Run the SQL query inside the Docker container
docker exec -i ${DOCKER_CONTAINER_NAME} psql -U ${DB_USER} -d ${DB_NAME} -c "${SQL_QUERY}"

# Check if the update was successful
if [ $? -eq 0 ]; then
  echo "Successfully updated is_signed_up and signup_time for client_id: ${CLIENT_ID}"
else
  echo "Failed to update the database for client_id: ${CLIENT_ID}"
fi

CLIENT_ID="1000007"
DOCKER_CONTAINER_NAME="temporal-postgresql"
DB_NAME="userstate"
TABLE_NAME="user_state"
DB_USER="temporal"

# SQL query to update the table
SQL_QUERY="UPDATE ${TABLE_NAME} SET is_signed_up = TRUE, signup_time = TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') WHERE client_id = '${CLIENT_ID}';"

# Run the SQL query inside the Docker container
docker exec -i ${DOCKER_CONTAINER_NAME} psql -U ${DB_USER} -d ${DB_NAME} -c "${SQL_QUERY}"

# Check if the update was successful
if [ $? -eq 0 ]; then
  echo "Successfully updated is_signed_up and signup_time for client_id: ${CLIENT_ID}"
else
  echo "Failed to update the database for client_id: ${CLIENT_ID}"
fi
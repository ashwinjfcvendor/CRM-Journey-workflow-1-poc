#!/bin/bash

CLIENT_ID="1000003"
DOCKER_CONTAINER_NAME="temporal-postgresql"
DB_NAME="userstate"
TABLE_NAME="user_state"
DB_USER="temporal"

# SQL query to update the table
SQL_QUERY="UPDATE ${TABLE_NAME} SET is_app_logged_in = TRUE, login_time = TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') WHERE client_id = '${CLIENT_ID}';"

# Run the SQL query inside the Docker container
docker exec -i ${DOCKER_CONTAINER_NAME} psql -U ${DB_USER} -d ${DB_NAME} -c "${SQL_QUERY}"

# Check if the update was successful
if [ $? -eq 0 ]; then
  echo "Successfully updated is_app_logged_in and login_time for client_id: ${CLIENT_ID}"
else
  echo "Failed to update the database for client_id: ${CLIENT_ID}"
fi

CLIENT_ID="1000007"
DOCKER_CONTAINER_NAME="temporal-postgresql"
DB_NAME="userstate"
TABLE_NAME="user_state"
DB_USER="temporal"

# SQL query to update the table
SQL_QUERY="UPDATE ${TABLE_NAME} SET click_email_4a = TRUE WHERE client_id = '${CLIENT_ID}';"

# Run the SQL query inside the Docker container
docker exec -i ${DOCKER_CONTAINER_NAME} psql -U ${DB_USER} -d ${DB_NAME} -c "${SQL_QUERY}"

# Check if the update was successful
if [ $? -eq 0 ]; then
  echo "Successfully updated click_email_4a for client_id: ${CLIENT_ID}"
else
  echo "Failed to update the database for client_id: ${CLIENT_ID}"
fi
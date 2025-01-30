
#!/bin/bash

# Variables for database and table creation
DB_NAME="userstate"
TABLE_NAME="user_state"

# PostgreSQL user and password
DB_USER="temporal"
DB_PASSWORD="temporal"

# PostgreSQL container name
CONTAINER_NAME="temporal-postgresql"

# Check if the database exists
echo "Checking if database '$DB_NAME' exists..."
DB_EXISTS=$(docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -tAc "SELECT 1 FROM pg_database WHERE datname='$DB_NAME'")

# Debug: Output the result of the DB check
echo "DB_EXISTS: '$DB_EXISTS'"

if [ '$DB_EXISTS' = '1' ]; then
    echo "Database '$DB_NAME' already exists. Skipping database creation."
else
    # Create the userstate database
    echo "Creating database '$DB_NAME'..."
    docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -c "CREATE DATABASE $DB_NAME;"
fi

# Check if the table exists in the database
echo "Checking if table '$TABLE_NAME' exists in database '$DB_NAME'..."
TABLE_EXISTS=$(docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -tAc "SELECT table_name FROM information_schema.tables WHERE table_name = '$TABLE_NAME'")

# Debug: Output the result of the table check
echo "TABLE_EXISTS: '$TABLE_EXISTS'"

if [ -z $TABLE_EXISTS ]; then
    echo "Table '$TABLE_NAME' does not exist. Creating table."
    
    # Create the user_state table
    docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -tAc "
CREATE TABLE IF NOT EXISTS $TABLE_NAME (
    client_id VARCHAR(255) PRIMARY KEY,
    triggered_sign_up_workflow BOOLEAN NOT NULL,
    triggered_login_workflow BOOLEAN NOT NULL,
    is_signed_up BOOLEAN NOT NULL,
    is_usage_app BOOLEAN NOT NULL,
    is_app_logged_in BOOLEAN NOT NULL,
    sent_push_notification_2 BOOLEAN NOT NULL,
    sent_content_card_1a BOOLEAN NOT NULL,
    sent_push_notification_3 BOOLEAN NOT NULL,
    sent_email_4a BOOLEAN NOT NULL,
    click_email_4a BOOLEAN NOT NULL,
    sent_email_4b BOOLEAN NOT NULL,
    sent_email_5 BOOLEAN NOT NULL,
    is_logged_in BOOLEAN NOT NULL,
    signup_time VARCHAR(255) NULL,
    login_time VARCHAR(255) NULL
);
"
else
    echo "Table '$TABLE_NAME' already exists. Skipping table creation."
fi

echo "Database and table creation process completed!"

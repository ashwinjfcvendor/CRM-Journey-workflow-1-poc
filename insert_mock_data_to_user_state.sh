#!/bin/bash

# Variables for the database and table
DB_NAME="userstate"
TABLE_NAME="user_state"

# PostgreSQL user and password
DB_USER="temporal"
DB_PASSWORD="temporal"

# PostgreSQL container name
CONTAINER_NAME="temporal-postgresql"

# Insert mock data into the user_state table
echo "Inserting mock data into table '$TABLE_NAME' in database '$DB_NAME'..."

docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -tAc "
INSERT INTO $TABLE_NAME (client_id, is_signed_up, is_usage_app, is_app_logged_in, sent_push_notification_2, sent_content_card_1a, sent_push_notification_3, sent_email_4a, click_email_4a, sent_email_4b, sent_email_5)
VALUES
    ('client_123', TRUE, TRUE, TRUE, TRUE, FALSE, TRUE, TRUE, FALSE, TRUE, FALSE),
    ('client_124', TRUE, FALSE, TRUE, FALSE, TRUE, FALSE, FALSE, TRUE, FALSE, TRUE),
    ('client_125', FALSE, TRUE, FALSE, TRUE, TRUE, TRUE, FALSE, FALSE, TRUE, TRUE),
    ('client_126', TRUE, TRUE, FALSE, FALSE, FALSE, TRUE, TRUE, FALSE, TRUE, TRUE);
"

echo "Mock data inserted successfully!"

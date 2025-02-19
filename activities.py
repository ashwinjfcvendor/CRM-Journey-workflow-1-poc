from datetime import timedelta, datetime
from temporalio import activity

from confluent_kafka import Producer
import clickhouse_connect

import psycopg2
from psycopg2.extras import DictCursor, execute_values
from dataclasses import dataclass
import json


@activity.defn
async def check_and_insert_postgres(client_ids):
    postgres_con = psycopg2.connect(
        database="userstate",
        user="temporal",
        password ="temporal",
        host="localhost",
        port=5432
    )
    cursor = postgres_con.cursor()
    table_name = "user_state"

    if not client_ids:
        return  # No clients to insert
    
    print(tuple(client_ids))

    # Fetch existing client_ids in one query
    query = f"SELECT client_id FROM {table_name} WHERE client_id IN %s"
    cursor.execute(query, (tuple(client_ids),))
    existing_ids = {row[0] for row in cursor.fetchall()}  # Convert to a set for fast lookup

    print(existing_ids)

    # Filter out existing client_ids
    new_client_ids = [cid for cid in client_ids if cid not in existing_ids]

    print(new_client_ids)

    if new_client_ids:
        # Bulk insert all new client_ids at once
        insert_query = f"""
            INSERT INTO {table_name} (
                client_id,
                triggered_sign_up_workflow,
                triggered_login_workflow,
                is_signed_up, 
                is_usage_app, 
                is_app_logged_in, 
                sent_push_notification_2, 
                sent_content_card_1a, 
                sent_push_notification_3, 
                sent_email_4a, 
                click_email_4a, 
                sent_email_4b, 
                sent_email_5,
                is_logged_in,
                signup_time,
                login_time
            ) VALUES %s
        """

        # insert_query = f"""
        #     INSERT INTO {table_name} (
        #         client_id,
        #         triggered_sign_up_workflow,
        #         triggered_login_workflow,
        #         is_signed_up, 
        #         is_usage_app, 
        #         is_app_logged_in, 
        #         sent_push_notification_2, 
        #         sent_content_card_1a, 
        #         sent_push_notification_3, 
        #         sent_email_4a, 
        #         click_email_4a, 
        #         sent_email_4b, 
        #         sent_email_5,
        #         is_logged_in
        #     ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        # """

        values = [
            (cid, False, False, False, True, False, False, False, False, False, False, False, False, False, None, None)
            for cid in new_client_ids
        ]

        print(values)
        
        execute_values(cursor, insert_query, values)  # Bulk insert
        postgres_con.commit()
        print(f"Inserted {len(new_client_ids)} new entries.")

        return new_client_ids

@dataclass
class SessionCountInput:
     client_id: str
     start_time: str
     end_time: str

# Define Clickhouse activity for querying the number of sessions
@activity.defn(name="get_session_count")
async def get_session_count(input: SessionCountInput) -> int:
    # ClickHouse connection
    clickhouse_client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='')
    query = """
        SELECT COUNT(DISTINCT(session_id))
        FROM user_engagement_events_transformed
        WHERE client_id = %(client_id)s
          AND session_time >= %(start_time)s
          AND session_time <= %(end_time)s
    """
    start_time = datetime.strptime(input.start_time, "%Y-%m-%d %H:%M:%S")
    end_time = datetime.strptime(input.end_time, "%Y-%m-%d %H:%M:%S")
    params = {
        'client_id': input.client_id,
        'start_time': start_time,
        'end_time': end_time
    }
    result = clickhouse_client.query(query, parameters=params)
    data = result.result_rows
    return data[0][0]  # Assuming the result is a single count

@dataclass
class KafkaInput:
     topic: str
     event_data: dict

@activity.defn(name="send_kafka_event")
async def send_kafka_event(input: KafkaInput):
    """Send an event to Kafka."""
    # Kafka producer configuration
    kafka_config = {
        'bootstrap.servers': 'localhost:9092',
    }
    producer = Producer(kafka_config)
    producer.produce(input.topic, key=str(input.event_data['client_id']), value=json.dumps(input.event_data))
    producer.flush()

@activity.defn(name="get_login_eligible_client_ids")
async def get_login_workflow_eligible_clients():
    postgres_con = psycopg2.connect(
        database="userstate",
        user="temporal",
        password ="temporal",
        host="localhost",
        port=5432
    )
    cursor = postgres_con.cursor()
    query = "SELECT client_id, signup_time FROM user_state WHERE triggered_login_workflow = False AND is_signed_up = True AND is_app_logged_in = False;"
    cursor.execute(query)
    login_eligible_client_ids = cursor.fetchall()
    return login_eligible_client_ids

@activity.defn(name="get_user_state")
async def get_user_state(client_id: str):
    postgres_con = psycopg2.connect(
        database="userstate",
        user="temporal",
        password ="temporal",
        host="localhost",
        port=5432
    )
    cursor = postgres_con.cursor(cursor_factory=DictCursor)
    query = "SELECT * FROM user_state WHERE client_id = %s;"
    cursor.execute(query,(client_id,))
    row = cursor.fetchone()
    if row:
            # Map the database row to the desired user_state format
            user_state = {
                "is_signed_up": row.get("is_signed_up"),
                "is_usage_app": row.get("is_usage_app"),
                "is_app_logged_in": row.get("is_app_logged_in"),
                "sent_push_notification_2": row.get("sent_push_notification_2"),
                "sent_content_card_1a": row.get("sent_content_card_1a"),
                "sent_push_notification_3": row.get("sent_push_notification_3"),
                "sent_email_4a": row.get("sent_email_4a"),
                "click_email_4a": row.get("click_email_4a"),
                "sent_email_4b": row.get("sent_email_4b"),
                "sent_email_5": row.get("sent_email_5"),
                "is_logged_in": row.get("is_logged_in"),
                "signup_time": row.get("signup_time"),
                "login_time": row.get("login_time")
            }
            return user_state
    else:
            return None

@dataclass
class UpdateUserStateInput:
     client_id: str
     event: str

@activity.defn(name="update_user_state")
async def update_user_state(input: UpdateUserStateInput):
    postgres_con = psycopg2.connect(
        database="userstate",
        user="temporal",
        password ="temporal",
        host="localhost",
        port=5432
    )
    cursor = postgres_con.cursor(cursor_factory=DictCursor)
    query = f"UPDATE user_state SET {input.event} = %s WHERE client_id = %s;"
    cursor.execute(query, (True, input.client_id))
    postgres_con.commit()
    return print(f"Updated user_state for {input.event}")

@activity.defn(name="query_clickhouse")
async def query_clickhouse():
    # ClickHouse connection
    clickhouse_client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='')
    query = "SELECT client_id, CAST(MIN(session_time) AS String) AS first_session_time FROM user_engagement_events_transformed WHERE session_time >= now() - INTERVAL 60 MINUTE GROUP BY client_id"

    result = clickhouse_client.query(query)
    data = result.result_rows

    if data:
        # client_ids = [row[0] for row in data]

        # # Check and insert into PostgreSQL
        # check_and_insert_postgres(client_ids)        
        # return client_ids
        return data
    return []
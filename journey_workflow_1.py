from datetime import timedelta, datetime

from temporalio import workflow, activity
from temporalio.worker import Worker
from temporalio.client import Client
import asyncio

from confluent_kafka import Producer
import clickhouse_connect

import psycopg2
from psycopg2.extras import DictCursor

postgres_con = psycopg2.connect(
    database="userstate",
    user="temporal",
    password ="temporal",
    host="localhost",
    port=5432
    )


# Kafka producer configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
}
producer = Producer(kafka_config)

# ClickHouse connection
clickhouse_client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='')

def check_and_insert_postgres(client_ids):
    cursor = postgres_con.cursor()
    table_name = "user_state"

    if not client_ids:
        return  # No clients to insert

    # Fetch existing client_ids in one query
    query = f"SELECT client_id FROM {table_name} WHERE client_id IN %s"
    cursor.execute(query, (tuple(client_ids),))
    existing_ids = {row[0] for row in cursor.fetchall()}  # Convert to a set for fast lookup

    # Filter out existing client_ids
    new_client_ids = [cid for cid in client_ids if cid not in existing_ids]

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
                is_logged_in
            ) VALUES %s
        """

        values = [
            (cid, False, False, False, True, False, False, False, False, False, False, False, False, False)
            for cid in new_client_ids
        ]
        
        psycopg2.extras.execute_values(cursor, insert_query, values)  # Bulk insert
        print(f"Inserted {len(new_client_ids)} new entries.")


def get_first_session_time(client_id:str):
    query = """
        SELECT MIN(toInt64(session_id)) AS lowest_session_id
        FROM user_engagement_events_transformed
        WHERE client_id = %s
    """
    result = clickhouse_client.query(query, (client_id,))
    data = result.result_rows
    if data and data[0][0] is not None:
        return data[0][0]

def sign_up_time(client_id:str):
    cursor = postgres_con.cursor()
    
    query = """
        SELECT signup_time 
        FROM user_state 
        WHERE client_id = %s
    """

    cursor.execute(query, (client_id,))
    data = cursor.fetchone()

    if data:
        return data[0]  # Return signup time

# Define Clickhouse activity for querying the number of sessions
@activity.defn(name="get_session_count")
def get_session_count(client_id: str, start_time: datetime, end_time: datetime) -> int:
    query = """
        SELECT COUNT(*)
        FROM sessions
        WHERE client_id = %(client_id)s
          AND session_time >= %(start_time)s
          AND session_time <= %(end_time)s
    """
    params = {
        'client_id': client_id,
        'start_time': start_time,
        'end_time': end_time
    }
    result = clickhouse_client.query(query, parameters=params)
    return result[0][0]  # Assuming the result is a single count


@activity.defn(name="send_kafka_event")
def send_kafka_event(topic: str, event_data: dict):
    """Send an event to Kafka."""
    producer.produce(topic, key=str(event_data['client_id']), value=str(event_data))
    producer.flush()

@activity.defn(name="get_user_state")
def get_user_state(client_id: str):
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

@activity.defn(name="update_user_state")
def update_user_state(client_id:str, event:str):
    cursor = postgres_con.cursor(cursor_factory=DictCursor)
    query = f"UPDATE user_state SET {event} = %s WHERE client_id = %s;"
    cursor.execute(query, (True, client_id))
    return print(f"Updated user_state for {event}")

@activity.defn(name="query_clickhouse")
def query_clickhouse():
    query = """
        SELECT DISTINCT client_id
        FROM user_engagement_events_transformed
        WHERE consumed_at >= now() - INTERVAL 10 MINUTE
    """ # between trigger time - 10 mins

    result = clickhouse_client.query(query)
    data = result.result_rows

    if data:
        client_ids = [row[0] for row in data]

        # Check and insert into PostgreSQL
        check_and_insert_postgres(client_ids)        
        return client_ids
    
    return []
    

# Workflow to handle the session data
@workflow.defn(name="customer_sign_up_workflow")
class CustomerSignUpWorkflow:
    @workflow.run
    async def run(self, client_id: str, first_session_time: datetime, ):
        user_state = await workflow.execute_activity(
            get_user_state,
            client_id,
            schedule_to_close_timeout=timedelta(seconds=10),
        )

        if not user_state["triggered_sign_up_workflow"] and not user_state["is_signed_up"]:

            await workflow.execute_activity(
                        update_user_state,
                        client_id,
                        "triggered_sign_up_workflow",
                        start_to_close_timeout=timedelta(seconds=5),
                    )

            ## Wait for 3 days to check if no sessions have occurred
            three_days_from_first_session_time = first_session_time + timedelta(days=3)
            seconds_to_three_days = round((three_days_from_first_session_time - datetime.now()).total_seconds())
            await asyncio.sleep(seconds_to_three_days)

            user_state = await workflow.execute_activity(
                get_user_state,
                client_id,
                schedule_to_close_timeout=timedelta(seconds=10),
            )
            
            if user_state["is_usage_app"]:
                ## Query Clickhouse to get session count for user
                sessions_last_3_days = await workflow.execute_activity(
                    get_session_count,
                    client_id=client_id,
                    start_time=first_session_time,
                    end_time=three_days_from_first_session_time,
                    schedule_to_close_timeout=timedelta(seconds=30)
                )

                ## Get the User Sign Up status from Aerospike (or equivalent)
                "Insert Activity"

                if not user_state["is_signed_up"] and sessions_last_3_days == 0:
                    workflow.logger.info("No sessions in the last 3 days. Sending Push notification event.")
                    # Send push notification 2 event to Kafka
                    await workflow.execute_activity(
                        send_kafka_event,
                        "send_push_notification_2",
                        {"client_id": client_id, "event": "sent_pushnotification_2"},
                        start_to_close_timeout=timedelta(seconds=5),
                    )
                    await workflow.execute_activity(
                        update_user_state,
                        client_id,
                        "sent_push_notification_2",
                        start_to_close_timeout=timedelta(seconds=5),
                    )

                    ## Update state of the user in Aerospike
                    "Insert Activity"
                    user_state["sent_push_notification_2"] == True

            # Wait for 5 days to send push notification 3
            five_days_from_first_session_time = first_session_time + timedelta(days=5)
            seconds_to_five_days = round((five_days_from_first_session_time - datetime.now()).total_seconds())
            await asyncio.sleep(seconds_to_five_days)

            user_state = await workflow.execute_activity(
                get_user_state,
                client_id,
                schedule_to_close_timeout=timedelta(seconds=10),
            )

            if user_state["is_usage_app"] and not user_state["is_app_logged_in"]:
                # sessions_last_5_days = await workflow.execute_activity(
                #     get_session_count,
                #     client_id=client_id,
                #     start_time=first_session_time,
                #     end_time=five_days_from_first_session_time,
                #     schedule_to_close_timeout=timedelta(seconds=30)
                # )

                ## Get the User Sign Up status from Aerospike (or equivalent)
                "Insert Activity"

                if not user_state["is_signed_up"]:
                    workflow.logger.info("No sign up after 5 days from first session. Sending Push notifiation 3 event.")
                    # Send notification event to Kafka
                    await workflow.execute_activity(
                        send_kafka_event,
                        "sent_push_notification_3",
                        {"client_id": client_id, "event": "sent_push_notification_3"},
                        start_to_close_timeout=timedelta(seconds=5),
                    )
                    await workflow.execute_activity(
                        update_user_state,
                        client_id,
                        "sent_push_notification_3",
                        start_to_close_timeout=timedelta(seconds=5),
                    )

@workflow.defn(name="show_content_card")
class ShowContentCardWorkflow:
    pass

@workflow.defn(name="customer_login_workflow")
class CustomerLoginWorkflow:
    @workflow.run
    async def run(self, client_id: str, signedup_time: datetime):
        user_state = await workflow.execute_activity(
            get_user_state,
            client_id,
            schedule_to_close_timeout=timedelta(seconds=10),
        )

        if not user_state["triggered_login_workflow"] and user_state["is_signed_up"] and not user_state["is_app_logged_in"]:
            
            await workflow.execute_activity(
                        update_user_state,
                        client_id,
                        "triggered_login_workflow",
                        start_to_close_timeout=timedelta(seconds=5),
                    )

            ## wait for 7 days after sighup
            seven_days_from_signup_time = signedup_time + timedelta(days=3)
            seconds_to_seven_days = round((seven_days_from_signup_time - datetime.now()).total_seconds())
            await asyncio.sleep(seconds_to_seven_days)

            user_state = await workflow.execute_activity(
                get_user_state,
                client_id,
                schedule_to_close_timeout=timedelta(seconds=10),
            )

            if not user_state["is_app_logged_in"]:
                workflow.logger.info("Customer has not app logged in the last 7 days. Sending email_4a")
                # send email_4a event to kafka
                await workflow.execute_activity(
                        send_kafka_event,
                        "send_email_4a",
                        {"client_id": client_id, "event": "send_email_4a"},
                        start_to_close_timeout=timedelta(seconds=5),
                    )
                await workflow.execute_activity(
                    update_user_state,
                    client_id,
                    "sent_email_4a",
                    start_to_close_timeout=timedelta(seconds=5),
                )

            ## Wait for 5 days after sending email_4a
            five_days_after_email_4a_sent = seven_days_from_signup_time + timedelta(days=5)
            seconds_to_five_days_after_email4a = round((five_days_after_email_4a_sent- datetime.now()).total_seconds())
            await asyncio.sleep(seconds_to_five_days_after_email4a)

            user_state = await workflow.execute_activity(
                get_user_state,
                client_id,
                schedule_to_close_timeout=timedelta(seconds=10),
            )

            if not user_state["is_app_logged_in"] and user_state["sent_email_4a"] and user_state["click_email_4a"]: 
                workflow.logger.info("Customer has not not app logged in for 5 days after email 4a has been sent but has opened email 4a. Sending email 5 coupon incentive")
                # send email_5 event to kafka
                await workflow.execute_activity(
                        send_kafka_event,
                        "send_email_5",
                        {"client_id": client_id, "event": "send_email_5"},
                        start_to_close_timeout=timedelta(seconds=5),
                    )
                await workflow.execute_activity(
                        update_user_state,
                        client_id,
                        "sent_email_5",
                        start_to_close_timeout=timedelta(seconds=5),
                    )

            elif not user_state["is_app_logged_in"] and user_state["sent_email_4a"] and not user_state["click_email_4a"]:
                workflow.logger.info("Customer has not not app logged in for 5 days after email 4a has been sent and also not opened email 4a. Sending email 4b")
                # send emai_4b to kafka
                await workflow.execute_activity(
                        send_kafka_event,
                        "send_email_4b",
                        {"client_id": client_id, "event": "send_email_4b"},
                        start_to_close_timeout=timedelta(seconds=5),
                    )
                await workflow.execute_activity(
                        update_user_state,
                        client_id,
                        "sent_email_4b",
                        start_to_close_timeout=timedelta(seconds=5),
                    )

@workflow.defn(name="main_workflow")
class MainWorkflow:
    @workflow.run
    async def run(self):
        # Query Clickhouse for new client IDs
        client_ids = await workflow.execute_activity(
            query_clickhouse,
            schedule_to_close_timeout=timedelta(seconds=30),
        )

        # Trigger workflows for each client ID
        for client_id in client_ids:
            try:
                # Start or continue the Customer Sign-Up workflow
                await workflow.start_child_workflow(
                    CustomerSignUpWorkflow.run,
                    client_id=client_id,
                    first_session_time=get_first_session_time(client_id),
                    id=f"customer-sign-up-{client_id}",
                    task_queue="event-driven-task-queue",
                )

                # Start or continue the Customer Login workflow
                await workflow.start_child_workflow(
                    CustomerLoginWorkflow.run,
                    client_id=client_id,
                    sign_up_time=sign_up_time(client_id),
                    id=f"customer-login-{client_id}",
                    task_queue="event-driven-task-queue",
                )
            except workflow.ChildWorkflowAlreadyRunningError:
                workflow.logger.info(f"Workflow for client {client_id} is already running.")



async def main():
    # Connect to Temporal server
    client = await Client.connect("localhost:7233")

    # Start the worker
    worker = Worker(
        client,
        task_queue="event-driven-task-queue",
        workflows=[MainWorkflow, CustomerSignUpWorkflow, CustomerLoginWorkflow],
        activities=[query_clickhouse, get_session_count, send_kafka_event, get_user_state, update_user_state],
    )

    # Start worker in the background
    worker_task=asyncio.create_task(worker.run())

    # Generate a unique workflow ID by appending the current timestamp
    workflow_id = f"cutomer-sign-up-workflow-{datetime.now().isoformat()}"

    # Start the parent workflow
    try:
        await client.start_workflow(
        MainWorkflow.run,
        id="main-clickhouse-workflow",
        task_queue=workflow_id,
        cron_schedule="*/10 * * * *",  # Runs every 10 minutes
        )
        print(f"Workflow completed")
    except Exception as e:
        print(f"Failed to start workflow: {e}")
    finally:
        # Gracefully shut down the worker
        await worker.shutdown()  # Stop the worker
        await worker_task

if __name__ == "__main__":
    asyncio.run(main())

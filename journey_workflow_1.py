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
                "sent_email_5": row.get("sent_email_5")
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



async def main():
    # Connect to Temporal server
    client = await Client.connect("localhost:7233")

    # Start the worker
    worker = Worker(
        client,
        task_queue="event-driven-task-queue",
        workflows=[CustomerSignUpWorkflow, CustomerLoginWorkflow],
        activities=[get_session_count, send_kafka_event, get_user_state, update_user_state],
    )

    # Start worker in the background
    worker_task=asyncio.create_task(worker.run())

    # Generate a unique workflow ID by appending the current timestamp
    workflow_id = f"cutomer-sign-up-workflow-{datetime.now().isoformat()}"

    # Start the parent workflow
    try:
        result = await client.execute_workflow(
            CustomerSignUpWorkflow.run,
            client_id="test-client",
            first_session_time=datetime.now(),
            id=workflow_id,
            task_queue="event-driven-task-queue",
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

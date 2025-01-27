from datetime import timedelta, datetime

from temporalio import workflow, activity
from temporalio.worker import Worker
from temporalio.client import Client
import asyncio

from confluent_kafka import Producer
import clickhouse_connect


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


# Workflow to handle the session data
@workflow.defn(name="customer_sign_up_workflow")
class CustomerSignUpWorkflow:
    @workflow.run
    async def run(self, client_id: str, first_session_time: datetime):
        user_state = {
            "is_signed_up": False,
            "is_usage_app": False,
            "is_app_logged_in": False,
            "sent_push_notification_2": False,
            "sent_content_card_1a": False,
            "sent_push_notification_3": False,
            "sent_email_4a": False,
            "sent_email_4b": False,
            "sent_email_5": False
        }

        ## Wait for 3 days to check if no sessions have occurred
        three_days_from_first_session_time = first_session_time + timedelta(days=3)
        seconds_to_three_days = round((three_days_from_first_session_time - datetime.now()).total_seconds())
        await asyncio.sleep(seconds_to_three_days)
        
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
                    "send_notification_event",
                    {"client_id": client_id, "event": "send_notification"},
                    start_to_close_timeout=timedelta(seconds=5),
                )

                ## Update state of the user in Aerospike
                "Insert Activity"
                user_state["sent_push_notification_2"] == True

        # Wait for 5 days to send push notification 3
        five_days_from_first_session_time = first_session_time + timedelta(days=5)
        seconds_to_five_days = round((five_days_from_first_session_time - datetime.now()).total_seconds())
        await asyncio.sleep(seconds_to_five_days)

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
                    "send_content_card_event",
                    {"client_id": client_id, "event": "send_content_card"},
                    start_to_close_timeout=timedelta(seconds=5),
                )

@workflow.defn(name="show_content_card")
class ShowContentCardWorkflow:
    pass


async def main():
    # Connect to Temporal server
    client = await Client.connect("localhost:7233")

    # Start the worker
    worker = Worker(
        client,
        task_queue="event-driven-task-queue",
        workflows=[CustomerSignUpWorkflow],
        activities=[get_session_count, send_kafka_event],
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

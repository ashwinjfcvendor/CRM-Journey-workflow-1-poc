#import asyncio
import random
import time
from datetime import timedelta, datetime
from temporalio import workflow, activity
from temporalio.worker import Worker
from temporalio.client import Client
import uuid
import asyncio

# Generate random session data
async def query_ch() -> list:
    print("Querying CH")
    
    # Possible values for each field
    statuses = ["true", "false"]
    eligible_values = ["true", "false"]
    length = 5
    
    # Randomly generate session data
    var = [
        {
            "session_id": str(uuid.uuid4()).replace('-', '')[:length],
            "first_session_time": '2025-01-11T16:09:00',
            "cc1_status": random.choice(statuses),
            "eligible": random.choice(eligible_values),
        }
        for _ in range(2)  # Generate 2 random sessions
    ]
    
    return var

# Activity to simulate querying ClickHouse
@activity.defn
async def query_clickhouse() -> list:
    print("Querying ClickHouse for session data")
    var = await query_ch()
    print(var)
    return var

# Activity to calculate time difference between session start and current time
@activity.defn
async def calculate_time_difference(session: dict) -> str:
    first_session_time = datetime.strptime(session["first_session_time"], "%Y-%m-%dT%H:%M:%S")
    current_time = datetime.now()
    time_diff = current_time - first_session_time
    return f"Session {session['session_id']} has a time difference of {time_diff}"

# Activity to print the cc1_status (content card 1) if it is "true"
@activity.defn
async def print_cc1_status(session: dict) -> None:
    if session["cc1_status"] == "true":
        print(f"Sending contect card to--> {session['session_id']}")
    else:
        #await asyncio.sleep(5)  # Non-blocking delay
        print(f"content card has been sent already for {session['session_id']}")

# Workflow to handle the session data
@workflow.defn
class EventDrivenWorkflow:
    @workflow.run
    async def run(self):
        processed_sessions = set()  # To track already processed session IDs
        loop_count = 0  # Add a loop count for test purpose

        while loop_count < 3:  # Limit the number of loop iterations for testing
            # Query ClickHouse to get session data
            session_data = await workflow.execute_activity(
                query_clickhouse,
                schedule_to_close_timeout=timedelta(seconds=10),
            )

            # Filter for new and eligible sessions
            new_sessions = [
                session for session in session_data
                if session["session_id"] not in processed_sessions and session.get("eligible") == "true"
            ]

            # Mark new sessions as processed
            processed_sessions.update(session["session_id"] for session in new_sessions)

            # Start workflows for new sessions
            if new_sessions:
                await asyncio.gather(
                    *[
                        workflow.execute_child_workflow(
                            SessionWorkflow.run,
                            session,
                            id=f"session-workflow-{session['session_id']}",
                            task_queue="event-driven-task-queue",
                        )
                        for session in new_sessions
                    ]
                )

            # Wait before querying again (e.g., every 30 seconds)
            await asyncio.sleep(5)

            loop_count += 1  # Increment the loop count

# Workflow to process a single session
@workflow.defn
class SessionWorkflow:
    @workflow.run
    async def run(self, session: dict):
        # Calculate the time difference
        time_diff = await workflow.execute_activity(
            calculate_time_difference,
            session,
            schedule_to_close_timeout=timedelta(seconds=10),
        )
        print(time_diff)

        # Print cc1_status if it is "true"
        await workflow.execute_activity(
            print_cc1_status,
            session,
            schedule_to_close_timeout=timedelta(seconds=20),
        )


async def main():
    # Connect to Temporal server
    client = await Client.connect("localhost:7233")

    # Start the worker
    worker = Worker(
        client,
        task_queue="event-driven-task-queue",
        workflows=[EventDrivenWorkflow, SessionWorkflow],
        activities=[query_clickhouse, calculate_time_difference, print_cc1_status],
    )

    # Start worker in the background
    worker_task=asyncio.create_task(worker.run())

    # Generate a unique workflow ID by appending the current timestamp
    workflow_id = f"event-driven-workflow-{int(time.time())}"

    # Start the parent workflow
    try:
        result = await client.execute_workflow(
            EventDrivenWorkflow.run,
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

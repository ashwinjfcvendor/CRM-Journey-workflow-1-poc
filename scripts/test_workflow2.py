import asyncio
from datetime import timedelta
from temporalio import workflow, activity
from temporalio.worker import Worker
from temporalio.client import Client
import time

async def query_ch() -> list:
    print("Querying CH")
    return [
        {"session_id": "abc", "first_session_time": "2025-01-15T10:00:00", "is_eligible": "True", "cc1_status": "false"},
        {"session_id": "def", "first_session_time": "2025-01-15T10:05:00", "is_eligible": "True", "cc1_status": "true"},
        {"session_id": "arun", "first_session_time": "2025-01-15T10:05:00", "is_eligible": "True", "cc1_status": "true"},
    ]

# Activity to simulate querying ClickHouse
@activity.defn
async def query_clickhouse() -> list:
    print("Querying ClickHouse for session data")
    return await query_ch()

# Activity to calculate time difference between session start and current time
@activity.defn
async def calculate_time_difference(session: dict) -> str:
    from datetime import datetime
    
    first_session_time = datetime.strptime(session["first_session_time"], "%Y-%m-%dT%H:%M:%S")
    current_time = datetime.now()
    time_diff = current_time - first_session_time
    return f"Session {session['session_id']} has a time difference of {time_diff}"

# Activity to print the cc1_status(contenct card 1) if it is "true"
@activity.defn
async def print_cc1_status(session: dict) -> None:
    if session["cc1_status"] == "true":
        print(f"Session {session['session_id']} has cc1_status: {session['cc1_status']}")
    else:
        await asyncio.sleep(20)  # Non-blocking delay
        print(await query_ch())

# Workflow to handle the session data
@workflow.defn
class EventDrivenWorkflow:
    @workflow.run
    async def run(self):
        # Query ClickHouse to get session data
        session_data = await workflow.execute_activity(
            query_clickhouse,
            schedule_to_close_timeout=timedelta(seconds=10),
        )
        
        # Start all session workflows in parallel
        await asyncio.gather(
            *[
                workflow.execute_child_workflow(
                    SessionWorkflow.run,
                    session,
                    id=f"session-workflow-{session['session_id']}",
                    task_queue="event-driven-task-queue",
                )
                for session in session_data
            ]
        )

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
            schedule_to_close_timeout=timedelta(seconds=30),
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
    asyncio.create_task(worker.run())

    # Start the parent workflow
    result = await client.execute_workflow(
        EventDrivenWorkflow.run,
        id="event-driven-workflow",
        task_queue="event-driven-task-queue",
    )
    print(f"Workflow completed with result: {result}")

if __name__ == "__main__":
    asyncio.run(main())

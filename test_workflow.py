import asyncio
from datetime import timedelta
from temporalio import workflow, activity
from temporalio.worker import Worker
from temporalio.client import Client

@activity.defn
async def query_clickhouse(identifier: str) -> dict:
    """Query ClickHouse for journey flags and metrics for the given identifier."""
    print(f"Querying ClickHouse for {identifier}")
    # Simulating ClickHouse query response
    return {"eligible_for_journey": identifier.startswith("eligible_")}

@workflow.defn
class EventDrivenWorkflow:
    @workflow.run
    async def run(self, identifier: str):
        # Query ClickHouse for flags and metrics
        result = await workflow.execute_activity(
            query_clickhouse,
            identifier,
            schedule_to_close_timeout=timedelta(seconds=10),
        )
        print(f"Workflow result for {identifier}: {result}")

async def main():
    # Connect to Temporal server
    client = await Client.connect("localhost:7233")

    # Start the worker
    worker = Worker(
        client,
        task_queue="event-driven-task-queue",
        workflows=[EventDrivenWorkflow],
        activities=[
            query_clickhouse,
        ],
    )

    # Start worker in the background
    asyncio.create_task(worker.run())

    # Run a test workflow with multiple identifiers
    identifiers = ["eligible_user_123", "eligible_user_124"]
    for i in identifiers:
        print(f"Starting workflow with identifier: {i}")
        result = await client.execute_workflow(
            EventDrivenWorkflow.run,
            i,  # Pass the current identifier
            id=f"workflow-{i}",  # Ensure unique workflow ID
            task_queue="event-driven-task-queue",
        )
        print(f"Workflow completed with result: {result}")

if __name__ == "__main__":
    asyncio.run(main())

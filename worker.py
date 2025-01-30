from temporalio.client import Client
from temporalio.worker import Worker
from temporalio import workflow
import asyncio

# Import activity, passing it through the sandbox without reloading the module
with workflow.unsafe.imports_passed_through():
    from activities import check_and_insert_postgres, get_login_workflow_eligible_clients, query_clickhouse, get_session_count, send_kafka_event, get_user_state, update_user_state
    from journey_workflow_1 import MainWorkflow, CustomerSignUpWorkflow, CustomerLoginWorkflow

async def main():
    client = await Client.connect("localhost:7233")  # Connect to the Temporal service

    # Start the worker
    worker = Worker(
        client,
        task_queue="event-driven-task-queue",
        workflows=[MainWorkflow, CustomerSignUpWorkflow, CustomerLoginWorkflow],
        activities=[check_and_insert_postgres, get_login_workflow_eligible_clients, query_clickhouse, get_session_count, send_kafka_event, get_user_state, update_user_state],
    )

    await worker.run()  # Keeps running indefinitely

asyncio.run(main())
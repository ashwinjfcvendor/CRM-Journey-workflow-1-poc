from temporalio import workflow
from temporalio.worker import Worker
from temporalio.client import Client
from temporalio.workflow import ParentClosePolicy
import asyncio
from dataclasses import dataclass
from datetime import timedelta, datetime, timezone

import os
os.environ["TZ"] = "UTC"

# Import activity, passing it through the sandbox without reloading the module
with workflow.unsafe.imports_passed_through():
    from activities import check_and_insert_postgres, get_login_workflow_eligible_clients, query_clickhouse, get_session_count, send_kafka_event, get_user_state, update_user_state, KafkaInput, UpdateUserStateInput, SessionCountInput

@dataclass
class SignUpInput:
    client_id: str
    first_session_time_str: str

# Workflow to handle the session data
@workflow.defn(name="customer_sign_up_workflow")
class CustomerSignUpWorkflow:
    @workflow.run
    async def run(self, input: SignUpInput):
        user_state = await workflow.execute_activity(
            get_user_state,
            input.client_id,
            schedule_to_close_timeout=timedelta(seconds=10),
        )

        if not user_state["is_signed_up"]:
            await workflow.execute_activity(
                send_kafka_event,
                KafkaInput("trigger_braze_event", {"client_id": input.client_id, "event": "sent_content_card_1a"}),
                start_to_close_timeout=timedelta(seconds=5),
            )
            await workflow.execute_activity(
                update_user_state,
                UpdateUserStateInput(input.client_id, "sent_content_card_1a"),
                start_to_close_timeout=timedelta(seconds=5),
            )

        # if not user_state["triggered_sign_up_workflow"] and not user_state["is_signed_up"]:

        #     await workflow.execute_activity(
        #                 update_user_state,
        #                 client_id,
        #                 "triggered_sign_up_workflow",
        #                 start_to_close_timeout=timedelta(seconds=5),
        #             )

        first_session_time = datetime.strptime(input.first_session_time_str, "%Y-%m-%d %H:%M:%S")
        first_session_time = first_session_time.replace(tzinfo=timezone.utc)

        ## Wait for 3 days to check if no sessions have occurred
        three_days_from_first_session_time = first_session_time + timedelta(minutes=1)
        seconds_to_three_days = round((three_days_from_first_session_time - workflow.now()).total_seconds())
        await asyncio.sleep(seconds_to_three_days)

        user_state = await workflow.execute_activity(
            get_user_state,
            input.client_id,
            schedule_to_close_timeout=timedelta(seconds=10),
        )
        
        if user_state["is_usage_app"] and not user_state["is_signed_up"]:
            first_session_time_activity = first_session_time.strftime("%Y-%m-%d %H:%M:%S")
            three_days_from_first_session_time_activity = three_days_from_first_session_time.strftime("%Y-%m-%d %H:%M:%S")
            ## Query Clickhouse to get session count for user
            sessions_last_3_days = await workflow.execute_activity(
                get_session_count,
                SessionCountInput(input.client_id, first_session_time_activity, three_days_from_first_session_time_activity),
                schedule_to_close_timeout=timedelta(seconds=30)
            )

            if sessions_last_3_days == 1:
                workflow.logger.info("No sessions in the last 3 days. Sending Push notification event.")
                # Send push notification 2 event to Kafka
                await workflow.execute_activity(
                    send_kafka_event,
                    KafkaInput("trigger_braze_event", {"client_id": input.client_id, "event": "sent_pushnotification_2"}),
                    start_to_close_timeout=timedelta(seconds=5),
                )
                await workflow.execute_activity(
                    update_user_state,
                    UpdateUserStateInput(input.client_id, "sent_push_notification_2"),
                    start_to_close_timeout=timedelta(seconds=5),
                )

        # Wait for 5 days to send push notification 3
        five_days_from_first_session_time = first_session_time + timedelta(minutes=3)
        seconds_to_five_days = round((five_days_from_first_session_time - workflow.now()).total_seconds())
        await asyncio.sleep(seconds_to_five_days)

        user_state = await workflow.execute_activity(
            get_user_state,
            input.client_id,
            schedule_to_close_timeout=timedelta(seconds=10),
        )

        if user_state["is_usage_app"] and not user_state["is_signed_up"]:
            # sessions_last_5_days = await workflow.execute_activity(
            #     get_session_count,
            #     client_id=client_id,
            #     start_time=first_session_time,
            #     end_time=five_days_from_first_session_time,
            #     schedule_to_close_timeout=timedelta(seconds=30)
            # )
            workflow.logger.info("No sign up after 5 days from first session. Sending Push notifiation 3 event.")
            # Send notification event to Kafka
            await workflow.execute_activity(
                send_kafka_event,
                KafkaInput("trigger_braze_event", {"client_id": input.client_id, "event": "sent_push_notification_3"}),
                start_to_close_timeout=timedelta(seconds=5),
            )
            await workflow.execute_activity(
                update_user_state,
                UpdateUserStateInput(input.client_id, "sent_push_notification_3"),
                start_to_close_timeout=timedelta(seconds=5),
            )

@dataclass
class LoginInput:
    client_id: str
    signedup_time_str: str

@workflow.defn(name="customer_login_workflow")
class CustomerLoginWorkflow:
    @workflow.run
    async def run(self, input: LoginInput):
        # user_state = await workflow.execute_activity(
        #     get_user_state,
        #     client_id,
        #     schedule_to_close_timeout=timedelta(seconds=10),
        # )

        # if not user_state["triggered_login_workflow"] and user_state["is_signed_up"] and not user_state["is_app_logged_in"]:
            
        #     await workflow.execute_activity(
        #                 update_user_state,
        #                 client_id,
        #                 "triggered_login_workflow",
        #                 start_to_close_timeout=timedelta(seconds=5),
        #             )

        signedup_time = datetime.strptime(input.signedup_time_str, "%Y-%m-%d %H:%M:%S")
        signedup_time = signedup_time.replace(tzinfo=timezone.utc)

        ## wait for 7 days after sign up
        seven_days_from_signup_time = signedup_time + timedelta(minutes=1)
        seconds_to_seven_days = round((seven_days_from_signup_time - workflow.now()).total_seconds())
        await asyncio.sleep(seconds_to_seven_days)

        user_state = await workflow.execute_activity(
            get_user_state,
            input.client_id,
            schedule_to_close_timeout=timedelta(seconds=10),
        )

        if not user_state["is_app_logged_in"]:
            workflow.logger.info("Customer has not app logged in the last 7 days. Sending email_4a")
            # send email_4a event to kafka
            await workflow.execute_activity(
                send_kafka_event,
                KafkaInput("trigger_braze_event", {"client_id": input.client_id, "event": "send_email_4a"}),
                start_to_close_timeout=timedelta(seconds=5),
            )
            await workflow.execute_activity(
                update_user_state,
                UpdateUserStateInput(input.client_id, "sent_email_4a"),
                start_to_close_timeout=timedelta(seconds=5),
            )

        ## Wait for 5 days after sending email_4a
        five_days_after_email_4a_sent = seven_days_from_signup_time + timedelta(minutes=2)
        seconds_to_five_days_after_email4a = round((five_days_after_email_4a_sent- workflow.now()).total_seconds())
        await asyncio.sleep(seconds_to_five_days_after_email4a)

        user_state = await workflow.execute_activity(
            get_user_state,
            input.client_id,
            schedule_to_close_timeout=timedelta(seconds=10),
        )

        if not user_state["is_app_logged_in"] and user_state["sent_email_4a"] and user_state["click_email_4a"]: 
            workflow.logger.info("Customer has not not app logged in for 5 days after email 4a has been sent but has opened email 4a. Sending email 5 coupon incentive")
            # send email_5 event to kafka
            await workflow.execute_activity(
                    send_kafka_event,
                    KafkaInput("trigger_braze_event", {"client_id": input.client_id, "event": "send_email_5"}),
                    start_to_close_timeout=timedelta(seconds=5),
                )
            await workflow.execute_activity(
                    update_user_state,
                    UpdateUserStateInput(input.client_id, "sent_email_5"),
                    start_to_close_timeout=timedelta(seconds=5),
                )

        elif not user_state["is_app_logged_in"] and user_state["sent_email_4a"] and not user_state["click_email_4a"]:
            workflow.logger.info("Customer has not not app logged in for 5 days after email 4a has been sent and also not opened email 4a. Sending email 4b")
            # send emai_4b to kafka
            await workflow.execute_activity(
                    send_kafka_event,
                    KafkaInput("trigger_braze_event", {"client_id": input.client_id, "event": "send_email_4b"}),
                    start_to_close_timeout=timedelta(seconds=5),
                )
            await workflow.execute_activity(
                    update_user_state,
                    UpdateUserStateInput(input.client_id, "sent_email_4b"),
                    start_to_close_timeout=timedelta(seconds=5),
                )

@workflow.defn(name="main_workflow")
class MainWorkflow:
    @workflow.run
    async def run(self):
        # Query Clickhouse for new client IDs
        client_list = await workflow.execute_activity(
            query_clickhouse,
            schedule_to_close_timeout=timedelta(seconds=30),
        )

        client_ids = [row[0] for row in client_list]

        new_client_ids = await workflow.execute_activity(
            check_and_insert_postgres,
            client_ids,
            schedule_to_close_timeout=timedelta(seconds=120),
        )

        # if not new_client_ids:
        # Trigger workflows for each client ID
        for row in client_list:
            if row[0] in new_client_ids:
                try:
                    # Start or continue the Customer Sign-Up workflow
                    await workflow.start_child_workflow(
                        CustomerSignUpWorkflow.run,
                        SignUpInput(row[0], row[1]),
                        id=f"customer-sign-up-{row[0]}",
                        task_queue="event-driven-task-queue",
                        parent_close_policy=ParentClosePolicy.ABANDON
                    )
                # except workflow.ChildWorkflowAlreadyRunningError:
                #     workflow.logger.info(f"Workflow for client {row[0]} is already running.")
                except Exception as e:
                    print(e)
                
                await workflow.execute_activity(
                    update_user_state,
                    UpdateUserStateInput(row[0], "triggered_sign_up_workflow"),
                    start_to_close_timeout=timedelta(seconds=5),
                )
        
        print("Workflow will pause for a min. Kindly update the sign up status of the client_ids")
        await asyncio.sleep(120)

        login_eligible_client_ids = await workflow.execute_activity(
            get_login_workflow_eligible_clients,
            schedule_to_close_timeout=timedelta(seconds=120),
        )

        # if not login_eligible_client_ids:
        for row in login_eligible_client_ids:
            try:
                # Start or continue the Customer Login workflow
                await workflow.start_child_workflow(
                    CustomerLoginWorkflow.run,
                    LoginInput(row[0], row[1]),
                    id=f"customer-login-{row[0]}",
                    task_queue="event-driven-task-queue",
                    parent_close_policy=ParentClosePolicy.ABANDON
                )
            # except workflow.ChildWorkflowAlreadyRunningError:
            #         workflow.logger.info(f"Workflow for client {row[0]} is already running.")
            except Exception as e:
                    print(e)
                
            await workflow.execute_activity(
                update_user_state,
                UpdateUserStateInput(row[0], "triggered_login_workflow"),
                start_to_close_timeout=timedelta(seconds=5),
            )

async def main():
    # Connect to Temporal server
    client = await Client.connect("localhost:7233")

    # Generate a unique workflow ID by appending the current timestamp
    # workflow_id = f"customer-sign-up-workflow-{datetime.now().isoformat()}"

    # Start the parent workflow
    try:
        handle = await client.start_workflow(
            MainWorkflow.run,
            id="main-clickhouse-workflow",
            task_queue="event-driven-task-queue",
            # cron_schedule="*/10 * * * *",  # Runs every 10 minutes
        )
        print(handle)
        print(f"Workflow completed")
    except Exception as e:
        print(f"Failed to start workflow: {e}")
    # finally:
    #     # Gracefully shut down the worker
    #     await worker.shutdown()  # Stop the worker
    #     await worker_task

if __name__ == "__main__":
    asyncio.run(main())

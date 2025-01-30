docker compose up -d

echo ""
echo "Creating topics"
sleep 15
docker compose exec -it broker bash -c 'kafka-topics --bootstrap-server broker:29092 --create --topic user_engagement_events'
docker compose exec -it broker bash -c 'kafka-topics --bootstrap-server broker:29092 --create --topic dlq-user_engagement_events_transformed'
docker compose exec -it broker bash -c 'kafka-topics --bootstrap-server broker:29092 --create --topic trigger_braze_event'

echo ""
echo "Producing user engagement events to Kafka topic"
sleep 1
./venv/bin/python ./kafka-clients/producer.py --config ./kafka-clients/client.properties --topic user_engagement_events

echo ""
echo "Creating Flink Job to de-nestify the user engagement events"
sleep 2
docker compose exec -it flink-sql-client sql-client.sh -f /opt/flink/commands.sql

sleep 2
echo ""
echo "Listing all the topics in Kafka"
docker compose exec -it broker bash -c 'kafka-topics --bootstrap-server broker:29092 --list'

echo ""
echo "Creating a table in Clickhouse for ingestion"
docker compose exec -it clickhouse clickhouse-client --queries-file /var/lib/create-table.sql

sleep 10
echo ""
echo "Deploying a Clickhouse Sink Connector to write user engagement events to Clickhouse"
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @./clickhouse/connector-config.json

sleep 2
echo ""
echo "Status of the deployed clickhouse sink connector"
curl -s "http://localhost:8083/connectors?expand=status" | jq

sleep 1
echo ""
echo "Session Count per client id"
docker compose exec -it clickhouse clickhouse-client --query "SELECT client_id, COUNT(DISTINCT(session_id)) FROM user_engagement_events_transformed GROUP BY client_id"

sleep 2
echo ""

./create_user_state_db_table_in_postgres.sh

echo "Setup Completed"

echo ""
echo "Consuming from trigger_braze_event topic"
docker compose exec -it broker bash -c 'kafka-console-consumer --bootstrap-server broker:29092 --from-beginning --topic trigger_braze_event'
# Steps to run Clinkhouse Sink connector 

## 1. Run clickhouse instance and Connce worker instance

```bash
docker compose build
```

```bash
docker compose up -d
```

## 2. Verify the presence of clinkhouse sink connector plugin 

```bash
curl -s "http://localhost:8083/connector-plugins" | jq
```

## 3. Produce mock events using python script

* chechout kafka-clients directory for this 

## 4. Create a table in Clickhouse 

```bash
docker exec -it clickhouse bash
```

```bash
clickhouse-client
```

```bash
CREATE TABLE user_engagement_events (
    client_id String,
    event_name String,
    session_engaged String,
    page_location String,
    page_title String,
    session_count String,
    session_id String,
    page_referrer Nullable(String),
    consumed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY consumed_at;
```

## 5. Configure clickhouse sink connector 

```bash
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
  "name": "clickhouse-sink-connector",
  "config": {
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    "tasks.max": "1",
    "topics": "<kafka-topicname>",
    "hostname": "clickhouse",
    "port": "8123",
    "username": "default",
    "password": "",
    "database": "default",
    "table": "user_engagement_events",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "insert.mode": "insert",
    "auto.create": "false",
    "auto.evolve": "false",
    "errors.retry.timeout": "60",
    "errors.tolerance": "none",
    "errors.deadletterqueue.topic.name": "<ddl-topicname>",
    "errors.deadletterqueue.context.headers.enable": "true",
    "clickhouseSettings": "",
    "topic2TableMap": "<kafka-topicname>=user_engagement_events",
    "transforms": "ExtractEvents,Flatten",
    "transforms.ExtractEvents.type": "org.apache.kafka.connect.transforms.ExtractField$Value",
    "transforms.ExtractEvents.field": "payload.events",
    "transforms.Flatten.type": "org.apache.kafka.connect.transforms.Flatten$Value",
    "transforms.Flatten.delimiter": "_",
    "fields.whitelist": "client_id,name,params_session_engaged,params_page_location,params_page_title,params_session_count,params_session_id,params_page_referrer"
  }
}'
```

## 6. check status of the connector. 

```bash
curl -s "http://localhost:8083/connectors/clickhouse-sink-connector/status" | jq
```

## 7. Verify the values in the Clickhouse table

```bash
SELECT * FROM user_engagement_events;
```
# Steps to run Clinkhouse Sink connector 

## 1. Run clickhouse instance and Connce worker instance
* configure 'CONNECT_BOOTSTRAP_SERVERS', 'CONNECT_SASL_JAAS_CONFIG' in docker compose file

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
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @connector-config.json
```

## 6. check status of the connector. 

```bash
curl -s "http://localhost:8083/connectors/clickhouse-sink-connector/status" | jq
```

## 7. Verify the values in the Clickhouse table

```bash
SELECT * FROM user_engagement_events;
```
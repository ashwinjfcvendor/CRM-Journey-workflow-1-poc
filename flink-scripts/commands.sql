CREATE TABLE `user_engagement_events` (
    `schema` STRING,                                   -- Optional schema field
    `payload` ROW(                                     -- struct for the payload
        `events` ARRAY<ROW(                              -- Array of structs for events
            `name` STRING,                               -- Name of the event
            `params` ROW(                                -- Struct for event parameters
                `session_engaged` STRING,               -- Session engaged flag
                `page_location` STRING,                 -- Page location URL
                `page_title` STRING,                    -- Page title
                `session_count` STRING,                 -- Session count
                `session_id` STRING,                    -- Session ID
                `page_referrer` STRING                  -- Page referrer
            )
        )>,
        `client_id` STRING                               -- Client ID
    ),
    `ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'               -- Primary key, not enforced
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_engagement_events',
  'properties.bootstrap.servers' = 'broker:29092',
  'properties.group.id' = 'cliGroup',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'json'
);

CREATE TABLE `user_engagement_events_transformed` (
    `client_id` STRING,
    `event_name` STRING,
    `session_engaged` STRING,
    `page_location` STRING,
    `page_title` STRING,
    `session_count` STRING,
    `session_id` STRING,
    `page_referrer` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_engagement_events_transformed',
  'properties.bootstrap.servers' = 'broker:29092',
  'properties.group.id' = 'cliGroup',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'json',
  'sink.delivery-guarantee' = 'at-least-once',
  'sink.transactional-id-prefix' = 'cliTransaction'
);

INSERT INTO `user_engagement_events_transformed`
SELECT
  payload.client_id AS client_id,          
  payload.events[1].name AS event_name,
  payload.events[1].params.session_engaged AS session_engaged,
  payload.events[1].params.page_location AS page_location,
  payload.events[1].params.page_title AS page_title,
  payload.events[1].params.session_count AS session_count,
  payload.events[1].params.session_id AS session_id,
  payload.events[1].params.page_referrer AS page_referrer
FROM `user_engagement_events`;
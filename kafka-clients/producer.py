from confluent_kafka import Producer, Consumer
import json
import time
import argparse


def read_config(path):
  config = {}
  with open(path, "r") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

def produce(topic, config):
  producer = Producer(config)

  client_ids = ["1000001","10000002","1000003","1000004","1000005","1000006","1000007","1000008","1000009","1000010"]
  i =1
  session_count = 1

  for client_id in client_ids:
    for i in range (1,11):
      current_time = int(time.time())
      for session_count in range(1,11):
        if (i == 4 or i == 8) and client_id in ["1000003", "1000007"]:
          time.sleep(1)
        event = {
                  "name": "user_engagement",
                  "params": {
                      "session_engaged": "1",
                      "page_location": "https://dev-order.jollibee.com/en/ph",
                      "page_title": "Jollibee | Fast Food Restaurant Near Me",
                      "session_count": str(session_count),
                      "session_id": str(current_time),
                      "page_referrer": None
                  }
              }
        payload = {
                  "schema": None,
                  "payload": {
                      "events": [event],
                      "client_id": client_id
                  }
              }
        # key = "key"
        # value = "value"
        producer.produce(topic, value=json.dumps(payload))
        print(f"Produced message to topic {topic} value = {payload}")

        producer.flush()
        session_count = session_count+1
    i = i+1
    time.sleep(1)
  


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--topic", help="Name of topic to produce to",type=str)
  parser.add_argument("--config", help="Path of the configuration file",type=str)
  args = parser.parse_args()
  topic = args.topic
  config_path = args.config
  print(topic)
  config = read_config(config_path)

  produce(topic,config)
 
main()

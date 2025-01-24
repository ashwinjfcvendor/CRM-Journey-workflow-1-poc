# Steps to Produce GA events to kafka 

## 1. activate virtual env

## 2. Pip install the requirements.txt 

## 3. Configure the client.properties file to inclued
 * bootstrap.servers
 * sasl.username
 * sasl.password

## 4. Run the python producer script

```bash
python3 producer.py --topic <topicname>
```

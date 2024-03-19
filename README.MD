## Flink + Kafka Demo

### Prerequisites
- docker
- docker compose
- python

### Setup
- `docker compose up -d` to start Kafka cluster
- `pip install -r requirements.txt`
- download **flink-kafka-connector.jar** from [maven repository](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-kafka/3.1.0-1.18) and put it under `./resources` directory
- run `device_producer.py` to generate events
- run `flink_consumer.py` to process events from Kafka


### Stop
- stop `device_producer.py` script
- stop `flink_consumer.py` script
- `docker compose down`

---
Kafka Control Center is available on http://localhost:9021/
# Real Time Data Processing
Real-time data streaming enables you to analyze and process data in real time instead of waiting hours, days, or weeks to get answers. 
This architecture streams database changes from PostgreSQL to MySQL in real time using Kafka:

- PostgreSQL + Debezium capture change data (CDC) via logical replication.
- Kafka Connect sends CDC events to Kafka topics.
- Spark Structured Streaming processes and enriches the events.
- MySQL Sink Connector stores the raw data in MySQL.
- Schema Registry ensures data format consistency.
- Control Center and Prometheus monitor the entire pipeline

## Architecture design for Real Time Streaming 
![Diagram](/docs/images/architecture_realtime_streaming.png)


## Technology Used
* [x] **Apache Kafka and Spark Structured Streaming**
* [x] **Zookeeper, Confluentinc Connect,Schema Registry, Control Center, Provectuslabs Kafka UI**
* [x] **Python**
* [x] **MYSQL and PostgreSQL**
* [X] **Docker**


## Setup Instruction
1. Clone the repository and run docker-compose.yml file.

```bash
docker-compose up --build -d
```
2. Make sure to create **kafka-data**, **mysql**, **postgres**, **zookeeper** folder inside Data directory
3. Detailed document is available at  `docs/`
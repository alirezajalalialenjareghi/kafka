# kafka
# Kafka Order Processing Pipeline

A real-time order processing pipeline using Python, Apache Kafka, Apache Spark Structured Streaming, and PostgreSQL.

## Architecture

Python Producer → Kafka (`orders.events`) → Spark Structured Streaming → PostgreSQL

Invalid events are routed to Kafka (`orders.dlq`).

## Project Structure

- `docker-compose.yml` — Starts Kafka, Zookeeper, and PostgreSQL.
- `init.sql` — Creates the PostgreSQL orders table and indexes.
- `producer.py` — Generates and publishes random order events.
- `spark_consumer.py` — Processes Kafka events, validates data, calculates total amounts, and writes valid events to PostgreSQL.
- `README.md` — Project documentation.

## Requirements

- Docker and Docker Compose
- Python 3.12
- Java 17

## Setup

### 1. Start services

```bash
docker compose up -d
```

### 2. Create Kafka topics

```bash
docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --create --topic orders.events --partitions 1 --replication-factor 1

docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --create --topic orders.dlq --partitions 1 --replication-factor 1
```

### 3. Install Python dependencies

```bash
pip install kafka-python Faker pyspark==3.5.0
```

### 4. Run the producer

Generate 100 events:

```bash
python producer.py --count 100 --rate 1
```

Run continuously:

```bash
python producer.py --continuous --rate 2
```

### 5. Run the Spark consumer

```bash
python spark_consumer.py
```

## PostgreSQL Verification

```bash
docker exec -it orders-postgres psql -U user -d orders_db -c "SELECT * FROM orders LIMIT 10;"
```

## Features

- Random order event generation
- Kafka event streaming
- Structured Streaming with Spark
- Event validation
- Total amount calculation
- PostgreSQL persistence
- Invalid event routing to a Dead Letter Queue (DLQ)
- Spark checkpointing

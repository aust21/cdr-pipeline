# Call Records Data Pipeline
This project implements a data pipeline for streaming call records into a PostgreSQL database, leveraging Apache Kafka for message queuing, Elasticsearch for search and analytics, and Redis for caching. The pipeline is designed to handle high-throughput call record data, ensuring scalability, reliability, and efficient data processing.
## Architecture Overview

- Apache Kafka: Acts as the message broker to ingest and stream call records in real-time.
- Redis: Used as a caching layer to store frequently accessed call metadata, reducing database load.
- Elasticsearch: Indexes call records for fast search and analytics.
- PostgreSQL: Serves as the primary relational database for persistent storage of call records.
### Pipeline Components:
- Producer: A service that generates or ingests call records and publishes them to Kafka.
- Consumer: A service that subscribes to Kafka topics, processes records, and stores them in Redis, Elasticsearch, and PostgreSQL.
- Data Processor: Handles data validation, transformation, and enrichment before storage.



## Prerequisites

1. Docker: To run the services in containers.
2. Python 3.9+: For running producer and consumer scripts.
3. Java 11+: For Kafka and Elasticsearch.
4. PostgreSQL 14+: For the database.
5. Redis 7+: For caching.
6. Elasticsearch 8+: For search and analytics.
7. Kafka 3+: For message streaming.

## Setup Instructions

1. Clone the Repository:
```bash
git clone https://github.com/aust21/cdr-pipeline.git
cd cdr-pipeline
```

2. Install Dependencies:

```pip3 install -r requirements.txt```

3. Ensure Docker is installed and running.
4. Start the docker services
```bash
docker compose up -d
```

5. Stream data to kafka
```bash
python3 main.py 
```
6. Visit the redpanda console to view streamed data
```bash
http://localhost:8080/topics/ 
```


# Learn Kafka with Go

A hands-on project to learn Apache Kafka — produce and consume messages using Go.

## Architecture

```
+----------+       +-------+       +----------+
| Producer | ----> | Kafka | ----> | Consumer |
|  (Go)    |       |(KRaft)|       |  (Go)    |
+----------+       +-------+       +----------+
```

- **Kafka** — single-node cluster in KRaft mode (no ZooKeeper)
- **Producer** — Go service that sends JSON messages to a topic
- **Consumer** — Go service in a consumer group that reads messages

## Quick Start

```bash
docker compose up --build
```

This starts Kafka, a producer (sends 10 messages), and a consumer (reads them).

To rebuild after code changes:

```bash
docker compose up --build
```

## Run Locally (without Docker)

```bash
# Start only Kafka
docker compose up -d kafka

# Build and run the Go app
go build -o app .
./app produce   # in one terminal
./app consume   # in another terminal
```

## What You'll See

**Producer output:**
```
[PRODUCED]  key=order-1  value={"order_id": 1, "item": "book", "qty": 2}
[PRODUCED]  key=order-2  value={"order_id": 2, "item": "laptop", "qty": 1}
...
```

**Consumer output:**
```
[CONSUMED]  #1
  topic:     learn-topic
  partition: 0
  offset:    0
  key:       order-1
  value:     {"order_id": 1, "item": "book", "qty": 2}
...
```

## Key Kafka Concepts Covered

| Concept | Where |
|---------|-------|
| **Topic** | Created automatically as `learn-topic` |
| **Producer** | `producer/producer.go` — writes messages with keys |
| **Consumer Group** | `consumer/consumer.go` — reads with `learn-group` |
| **Partitions** | Default 1-partition topic (consumer output shows partition) |
| **Offsets** | Auto-committed every 1s; shows offset per message |
| **KRaft** | Kafka runs without ZooKeeper (`process.roles=broker,controller`) |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | `localhost:9092` | Kafka bootstrap server |
| `KAFKA_TOPIC` | `learn-topic` | Topic to produce/consume |

## Go Dependencies

- [`github.com/segmentio/kafka-go`](https://github.com/segmentio/kafka-go) — pure Go Kafka client (no CGO)

#!/bin/bash

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
cub kafka-ready -b broker:9092 1 20

# Create the topic
kafka-topics --create --topic users_created --bootstrap-server broker:9092 --partitions 1 --replication-factor 1

# Keep the script running to avoid container exit
tail -f /dev/null

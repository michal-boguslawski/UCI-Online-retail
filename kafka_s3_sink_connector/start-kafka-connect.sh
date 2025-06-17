# start-kafka-connect.sh
#!/bin/bash

python config_filler.py

export AWS_ACCESS_KEY_ID=$(cat /run/secrets/aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(cat /run/secrets/aws_secret_access_key)

/etc/confluent/docker/run &

# Wait for Kafka Connect REST API to be available
echo "Waiting for Kafka Connect to be ready..."
for i in {1..30}; do
  if curl -sf http://localhost:8083/; then
    echo "Kafka Connect is ready!"
    break
  fi
  echo "Waiting... ($i)"
  sleep 5
done

curl -X POST -H "Content-Type: application/json" --data @/home/appuser/s3-sink.json http://localhost:8083/connectors

wait

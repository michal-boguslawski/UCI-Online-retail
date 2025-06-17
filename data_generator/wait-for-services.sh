#!/bin/sh

echo "Waiting for schema registry to be available..."

while ! curl -s http://schema-registry:8081/subjects > /dev/null; do
  echo "Schema registry not available yet, retrying in 5 seconds..."
  sleep 5
done

echo "Schema registry is up!"


exec "$@"

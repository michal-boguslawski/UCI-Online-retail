#!/bin/sh
export AWS_ACCESS_KEY_ID=$(cat /run/secrets/aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(cat /run/secrets/aws_secret_access_key)
exec "$@"

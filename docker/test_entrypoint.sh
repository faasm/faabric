#!/bin/bash

set -e

# Run Redis in the background
REDIS_CONF=/code/docker/redis.conf
redis-server $REDIS_CONF
sleep 1s

exec "$@"

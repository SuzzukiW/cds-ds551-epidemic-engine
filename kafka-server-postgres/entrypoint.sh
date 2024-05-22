#!/bin/bash

# Wait for Postgres to start
while ! pg_isready -h postgres -p 5432 > /dev/null 2>&1; do
    echo "Connecting to PostgreSQL..."
    sleep 1
done

echo "Connected to PostgreSQL."

# Execute the CMD from the Dockerfile
exec "$@"

FROM python:3.8-slim

# Install PostgreSQL client (required for the entrypoint script to interact with PostgreSQL)
RUN apt-get update && \
    apt-get install -y libpq-dev gcc postgresql-client && \
    apt-get clean

# Set the working directory
WORKDIR /app

# Copy local code to the container
COPY . /app

# Install Python dependencies
RUN pip install --no-cache-dir kafka-python psycopg2-binary

# Copy the entrypoint script into the container and make it executable
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Set the entrypoint script to be executed
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

# Command to run the consumer script after the entrypoint script has finished
CMD ["python", "consumer_postgres.py"]

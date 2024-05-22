FROM python:3.8-slim

# install system updates and dependencies in a non-interactive mode
RUN apt-get update -y && DEBIAN_FRONTEND=noninteractive apt-get dist-upgrade -y

VOLUME /app
WORKDIR /app
COPY . /app

# Install Python dependencies
RUN pip install kafka-python

CMD ["python", "consumer.py"]

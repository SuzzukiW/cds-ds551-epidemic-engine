FROM bitnami/spark:3.3.2

# set the user to root, issues occur otherwise
USER root

# install required packages
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip curl && \
    apt-get clean;

# set the working directory
WORKDIR /app/spark-analytics

# copy the necessary files
COPY requirements.txt .
COPY model_trainer.py .
COPY anomaly_detection.py .
COPY anomaly_detection_trainer.py .
COPY healthcheck.py .

# install additional Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# set the entrypoint to run both scripts simultaneously
ENTRYPOINT ["sh", "-c", "python3 anomaly_detection.py & python3 model_trainer.py"]
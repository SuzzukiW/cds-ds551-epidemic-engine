# data-visualization/Dockerfile
FROM bitnami/spark:3.3.2

# set the user to root, issues occur otherwise
USER root

# install required packages
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip curl && \
    apt-get clean;

# set the working directory
WORKDIR /app/data-visualization/

# copy the necessary files
COPY . .

# install additional Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# set the entrypoint
EXPOSE 5000
ENTRYPOINT ["python3", "visualizations.py"]
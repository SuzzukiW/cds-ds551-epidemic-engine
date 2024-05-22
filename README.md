# CDS DS 593 Epidemic Engine

## Project 1

### Part 1: Batch Data Analysis with Hadoop

#### Tasks

* Write a MapReduce job to count the number of events by type.
  * Completed
* Aggregate events by location to identify high-risk areas.
  * Completed
* Update README.md with how to launch and run your solution.
  * To run the solution, you should simply use the following command from the hadoop directory:
    * Note that the Makefile will only successfully run on a Linux/Unix system due to the use of the `cat` command

    ```bash
    make hadoop_solved
    ```

  * The output will be printed to the console as per the TA's instructions (not stored to a local file)
  * A sample output is listed below

  ```bash
  | Bordeaux              | 211   |
  | Boston                | 267   |
  | Chicago               | 263   |
  | Tokyo                 | 259   |
  | General Health Report | 280   |
  | Health Mention        | 232   |
  | Hospital Admission    | 258   |
  | Pharmacy Sale         | 230   |

  ```

## Project 2

### Part 1: Data Ingestion and Categorization with Kafka

#### Notes

* The `kafka` python package does not work properly if using Python 3.12.
  * If we do want to use Python 3.12, we need to do `pip install git+https://github.com/dpkp/kafka-python.git` to install the latest version of the package directly from github which has the necessary fixes.

### Project Structure

* `consumer.py`: Python script that implements the Kafka consumer and producers for data ingestion and categorization.
* `docker-compose.yml`: Docker Compose file to set up the Kafka environment and run the consumer application.
* `Dockerfile`: Dockerfile to build the consumer application image.
* `README.md`: Comprehensive documentation of the project.

### Getting Started

1. Clone the repository:

   ```shell
   gh repo clone BU-CDS-DS539-2024-Spring/epidemic-engine-project-ethanwilliamxiang
   ```

2. Navigate to the project directory:

   ```shell
   cd kafka-server/
   ```

3. Start the Kafka environment and consumer application:

   ```shell
   docker compose up -d
   ```

4. Create the required topics using Kafka Manager:
   * Open your web browser and go to `http://localhost:9000`.
   * Click on "Add Cluster" and provide a name for your Kafka cluster.
   * Enter the Zookeeper connection string as `zookeeper:2181`.
   * Click on "Create Cluster".
   * Once the cluster is created, click on the "Topics" tab.
   * Create the following topics: `hospital_admission`, `emergency_incident`, `vaccination`, `low`, `medium`, `high`, `general_events`.

5. Monitor the consumer logs:

   ```shell
   docker compose logs -f consumer
   ```

### System Design

The system design follows a producer-consumer architecture using Apache Kafka. Here's an overview of the components and their roles:

* **Kafka Consumer**: The consumer (`consumer.py`) subscribes to the 'health_events' topic and consumes messages from it. It is responsible for parsing the messages, categorizing them based on event type and severity, and producing them to the appropriate topics. Now, it also stores each processed event into a SQLite database to allow for data persistence and retrieval.

* **Kafka Producers**: The system creates separate Kafka producers for each topic (`hospital_admission`, `emergency_incident`, `vaccination`, `low`, `medium`, `high`, `general_events`). The consumer uses these producers to send the categorized messages to their respective topics.

* **Kafka Topics**: The system utilizes multiple Kafka topics to segregate the health event data based on event type and severity. The topics are:
  * `hospital_admission`: Contains events related to hospital admissions.
  * `emergency_incident`: Contains events related to emergency incidents.
  * `vaccination`: Contains events related to vaccinations.
  * `low`: Contains events with low severity.
  * `medium`: Contains events with medium severity.
  * `high`: Contains events with high severity.
  * `general_events`: Contains events that do not match any specific event type or severity.

* **Kafka Cluster**: The Kafka cluster, set up using Docker Compose, consists of Zookeeper and Kafka broker services. It provides the necessary infrastructure for message streaming and storage.

* **Kafka Manager**: Kafka Manager is a web-based tool for managing and monitoring Kafka clusters. It is included in the Docker Compose setup and accessible at `http://localhost:9000`. Kafka Manager allows creating and managing Kafka topics.

### Data Flow

1. Health event data is produced to the 'health_events' topic by external systems.
2. The Kafka consumer (`consumer.py`) consumes messages from the 'health_events' topic.
3. For each consumed message, the consumer parses the message value as JSON and extracts the event type and severity.
4. Based on the event type and severity, the consumer determines the target topic for the message.
5. The consumer uses the corresponding Kafka producer to send the message to the target topic.
6. The categorized messages are stored in their respective topics for further processing or analysis, and now also n the SQLite database for storage.

### Logging

The system utilizes the Python `logging` module to log important information and errors during message processing. The logs are outputted to the console and can be monitored using the `docker compose logs -f consumer` command.

### Stopping the System

To stop the Kafka environment and consumer application, run the following command:

```shell
docker-compose down
```

This command will stop and remove the containers defined in the `docker-compose.yml` file.

## Final Project / Project 3 Overview

This project utilizes Docker Compose to efficiently manage and deploy all containers through a single configuration. Previously, the setup consisted of multiple Docker Compose files, which have now been consolidated into one comprehensive file, as orchestrated by the Makefile in the root directory.

### How to Run

To launch the project, simply execute the command `make` in the root directory. This command triggers the Makefile, ensuring any previously running containers that might interfere are shut down before launching new ones. To stop the project, use the command `make stop`.

### Accessing Visualization

To interact with the visualizations, follow these steps:

* Navigate to the docker containers and locate the `data-visualizations` container.
* Access the web interface by entering `localhost:5000` in your browser. This interface displays the visualizations generated by the project.

This setup streamlines the deployment process by consolidating the configuration into one Docker Compose file, simplifying management and deployment tasks.

* If you encounter any issues with the Kafka environment or consumer application, check the logs using the `docker compose logs` command.
* Make sure the required topics are created using Kafka Manager before running the consumer application.
* Ensure that the Kafka bootstrap servers and topic names in the `consumer.py` script match your Kafka setup.

## Kubernetes Deployment

### Prerequisites

* Docker: Make sure you have Docker installed on your machine.
* Minikube: Install Minikube by following the official documentation: https://minikube.sigs.k8s.io/docs/start/

### Setup

1. Start Minikube:
   ```
   minikube start
   ```

2. Verify Minikube installation:
   ```
   minikube status
   ```

3. Configure `kubectl`:
   ```
   kubectl cluster-info
   ```

4. Set up the Docker environment:
   ```
   eval $(minikube docker-env)
   ```

5. Build the Docker images for the project components:
   ```
   # kafka-server
   docker build -t your-dockerhub-username/kafka-consumer:latest ./kafka-server
   
   # spark-analytics
   docker build -t your-dockerhub-username/spark-analytics:latest ./spark-analytics
   
   # data-visualization
   docker build -t your-dockerhub-username/data-visualization:latest ./data-visualization
   ```

6. Apply the Kubernetes configuration files:
   ```
   kubectl apply -f kubernetes/data-visualization-deployment.yaml
   kubectl apply -f kubernetes/data-visualization-service.yaml
   kubectl apply -f kubernetes/kafka-deployment.yaml
   kubectl apply -f kubernetes/kafka-service.yaml
   kubectl apply -f kubernetes/spark-deployment.yaml
   kubectl apply -f kubernetes/spark-service.yaml
   ```

7. Verify the deployments:
   ```
   kubectl get deployments
   ```

### Cleanup

To delete the Kubernetes resources and stop Minikube, follow these steps:

1. Delete the deployments and services:
   ```
   kubectl delete -f kubernetes/data-visualization-deployment.yaml
   kubectl delete -f kubernetes/kafka-deployment.yaml
   kubectl delete -f kubernetes/spark-deployment.yaml
   kubectl delete -f kubernetes/data-visualization-service.yaml
   kubectl delete -f kubernetes/kafka-service.yaml
   kubectl delete -f kubernetes/spark-service.yaml
   ```

   Or, you can also delete all resources at once:
   ```
   kubectl delete -f kubernetes/
   ```

2. Verify that the resources have been deleted:
   ```
   kubectl get deployments
   kubectl get services
   ```

3. Stop Minikube:
   ```
   minikube stop
   ```

4. Delete the Minikube cluster (optional):
   ```
   minikube delete
   ```
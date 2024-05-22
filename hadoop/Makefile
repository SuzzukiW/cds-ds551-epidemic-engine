DOCKER_NETWORK = hadoop_default
ENV_FILE = hadoop.env
current_branch := 2.0.0-hadoop3.2.1-java8
CONTAINER_CMD = docker
RUN_PARAMS = --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} bde2020/hadoop-base:$(current_branch)
INPUT = "simulated_health_events.csv"
MAPPER = "word_count_python/mapper.py"
REDUCER = "word_count_python/reducer.py"

wordcount-java:
	$(CONTAINER_CMD) build -t hadoop-wordcount-java ./word_count_java
	$(CONTAINER_CMD) run $(RUN_PARAMS) hadoop fs -mkdir -p /input/
	$(CONTAINER_CMD) run $(RUN_PARAMS) hadoop fs -copyFromLocal -f /opt/hadoop-3.2.1/README.txt /input/
	$(CONTAINER_CMD) run --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} hadoop-wordcount-java
	$(CONTAINER_CMD) run $(RUN_PARAMS) hadoop fs -cat /output/*
	$(CONTAINER_CMD) run $(RUN_PARAMS) hadoop fs -rm -r /output
	$(CONTAINER_CMD) run $(RUN_PARAMS) hadoop fs -rm -r /input

wordcount-python:
	$(CONTAINER_CMD) build -t hadoop-wordcount-python ./word_count_python
	./add-python.sh
	$(CONTAINER_CMD) run $(RUN_PARAMS) hadoop fs -rm -r /output || : #ignore doesn't exist
	$(CONTAINER_CMD) run $(RUN_PARAMS) hadoop fs -rm -r /input  || : #ignore doesn't exist
	$(CONTAINER_CMD) run $(RUN_PARAMS) hadoop fs -mkdir -p /input/
	$(CONTAINER_CMD) run $(RUN_PARAMS) hadoop fs -copyFromLocal -f /opt/hadoop-3.2.1/README.txt /input/
	$(CONTAINER_CMD) run --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} hadoop-wordcount-python
	$(CONTAINER_CMD) run $(RUN_PARAMS) hadoop fs -cat /output/*
	$(CONTAINER_CMD) run $(RUN_PARAMS) hadoop fs -rm -r /input

event-counter: ;

location-mapreducer: ;

hadoop_solved:
	echo "Attempting to run mapper and reducer":
	echo "cat ${INPUT} | ${MAPPER} | sort | ${REDUCER} > ./output"
	cat ${INPUT} | python3 ${MAPPER} | sort | python3 ${REDUCER}
	echo Completed

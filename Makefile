CONTAINER_CMD = docker

all: stop
	$(CONTAINER_CMD) compose up --build --detach

stop:
	$(CONTAINER_CMD) compose down --remove-orphans

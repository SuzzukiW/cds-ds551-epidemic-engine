# data-visualization/Makefile

.PHONY: build run clean test

build:
    docker-compose build

run:
    docker-compose up -d

clean:
    docker-compose down
    docker-compose rm -f

test:
    pytest
.PHONY: build test run

build:
    docker-compose build

test:
    docker-compose run --rm airflow pytest tests/

run:
    docker-compose up

clean:
    docker-compose down
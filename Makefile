.PHONY: test clean

test:
	@echo "Stopping and removing existing test containers..."
	docker compose -f docker-compose.test.yml down -v

	@echo "Building test containers..."
	docker compose -f docker-compose.test.yml build

	@echo "Starting test containers and running tests..."
	docker compose -f docker-compose.test.yml up --exit-code-from test

clean:
	@echo "Cleaning up Docker resources..."
	docker compose -f docker-compose.test.yml down -v --rmi all

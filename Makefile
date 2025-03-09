.PHONY: test clean


build-services:
	@echo "Building Docker services..."
	docker compose -f docker-compose.test.yml build postgres-test minio-test superset superset-database

start-services:
	@echo "Starting Docker services..."
	docker compose -f docker-compose.test.yml up -d postgres-test minio-test superset superset-database

stop-services:
	@echo "Stopping Docker services..."
	docker compose -f docker-compose.test.yml down postgres-test minio-test superset superset-database

init-superset:
	@echo "Initializing Superset..."
	./setup.sh

test:
	@echo "Running tests..."
	docker compose -f docker-compose.test.yml up -d test

clean:
	@echo "Cleaning up Docker resources..."
	docker compose -f docker-compose.test.yml down -v --rmi all

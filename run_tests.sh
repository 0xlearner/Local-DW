#!/bin/bash
# File: run_tests.sh

set -e  # Exit immediately if a command exits with non-zero status

echo "Starting services check..."
python wait_for_services.py

echo "Running tests..."
pytest tests/test_pipeline.py::test_real_data_pipeline -v -s --cov=src --cov-report=term-missing | tee /app/logs/test_real_data_pipeline.log

# Check test exit status
TEST_EXIT_CODE=${PIPESTATUS[0]}  # Get pytest exit code, not tee
if [ $TEST_EXIT_CODE -ne 0 ]; then
    echo "Tests failed with exit code $TEST_EXIT_CODE"
    exit $TEST_EXIT_CODE
fi

echo "Tests completed successfully!"
exit 0

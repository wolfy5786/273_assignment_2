FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements
COPY sync-REST/tests/requirements.txt ./requirements.txt

COPY logging_utils ./logging_utils
COPY setup.py ./setup.py

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install -e .

# Copy test file
COPY sync-REST/tests/test_order_service.py ./test_order_service.py

# Set environment variable for order service URL
ENV ORDER_SERVICE_URL=http://order-service:8080

# Default command runs the tests
CMD ["pytest", "test_order_service.py", "-v", "-s", "--tb=short"]

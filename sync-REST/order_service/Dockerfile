FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY sync-REST/order_service/requirements.txt ./requirements.txt

COPY logging_utils ./logging_utils
COPY setup.py ./setup.py


# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install -e .


# Copy application code
COPY sync-REST/order_service/app.py ./app.py

# Set default environment variables (can be overridden)
ENV NOTIFICATION_SERVICE_URL=http://notification-service:8081
ENV INVENTORY_SERVICE_URL=http://inventory-service:8082

# Expose the port the app runs on
EXPOSE 8080

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python", "app.py"]

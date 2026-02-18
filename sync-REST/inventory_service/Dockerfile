FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY sync-REST/inventory_service/requirements.txt ./requirements.txt

COPY logging_utils ./logging_utils
COPY setup.py ./setup.py


# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install -e . 

# Copy application code
COPY sync-REST/inventory_service/app.py ./app.py


# Expose the port the app runs on
EXPOSE 8082

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python", "app.py"]
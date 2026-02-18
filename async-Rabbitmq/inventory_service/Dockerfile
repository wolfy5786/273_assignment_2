FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# utils/ and setup.py are volume-mounted at runtime
ENV PYTHONPATH=/app

CMD ["python", "app.py"]

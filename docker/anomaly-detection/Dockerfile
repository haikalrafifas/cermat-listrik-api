FROM python:3.11-slim

WORKDIR /app

COPY docker/anomaly-detection/requirements.txt .
RUN pip install -r requirements.txt

COPY app/ ./app

CMD ["python", "-m", "app.workers.anomaly_detection"]

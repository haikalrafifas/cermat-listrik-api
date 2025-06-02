FROM python:3.11-slim

WORKDIR /app

COPY docker/forecasting/requirements.txt .
RUN pip install -r requirements.txt

COPY app/ ./app

CMD ["gunicorn", "app.server:app", "--workers", "1", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000"]

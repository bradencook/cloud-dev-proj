FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

CMD ["python", "consumer.py", "-sq", "cs5270-requests", "-dwt", "widgets"]

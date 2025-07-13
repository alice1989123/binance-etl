FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Copy files
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY ./src ./src
COPY ./keys ./keys
COPY backfill_runner.sh .
RUN chmod +x /app/backfill_runner.sh
ENTRYPOINT ["/app/backfill_runner.sh"]

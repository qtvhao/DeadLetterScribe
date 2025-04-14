# Use official Python base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies (for Kafka client)
RUN apt-get update && apt-get install -y \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy your script into the container
COPY scribe.py .

# Install Python dependencies
RUN pip install --no-cache-dir confluent-kafka

# Set the default command to run the script
CMD ["python", "scribe.py"]

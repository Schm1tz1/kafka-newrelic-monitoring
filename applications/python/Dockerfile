FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY applications/python ./applications/python

# Non-root user (UID 1001 is the default non-root user in ubi9-minimal)
USER 1001

# MODULE is set by the compose service (e.g. applications.python.confluent_kafka.producer)
ENV MODULE=""

CMD python -m ${MODULE}

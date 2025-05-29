# Use official Python 3.12 slim image as base
FROM python:3.12-slim

# Set environment variables to prevent Python from buffering and to enable output flushing
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Set working directory in container
WORKDIR /app

# Install system dependencies needed for PostgreSQL and other packages
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# Copy requirements.txt and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code and entrypoint script
COPY . .
COPY entrypoint.sh /entrypoint.sh

# Make the entrypoint script executable
RUN chmod +x /entrypoint.sh

# Set default entrypoint
ENTRYPOINT ["/entrypoint.sh"]

## Default command can be overridden by docker-compose or CLI
#CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

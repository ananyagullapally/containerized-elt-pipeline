FROM python:3.11-slim

# Install system dependencies for Postgres and dbt
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the pipeline code and dbt project
COPY . .

# Run the pipeline
CMD ["python", "main.py"]

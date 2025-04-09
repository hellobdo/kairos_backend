FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies required for numpy and other packages
RUN apt-get update && apt-get install -y \
    build-essential \
    sqlite3 \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install wheel first and upgrade pip
RUN pip install --no-cache-dir --upgrade pip wheel setuptools

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and tests
COPY . .

# Start a bash shell by default
CMD ["/bin/bash"]
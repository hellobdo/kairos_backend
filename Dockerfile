FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies required for numpy and other packages
RUN apt-get update && apt-get install -y \
    build-essential \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies directly
RUN pip install --no-cache-dir \
    numpy>=1.26.0 \
    pandas>=2.1.0 \
    vectorbt>=0.24.5 \
    plotly>=5.18.0 \
    scikit-learn>=1.3.0 \
    scipy>=1.11.3 \
    SQLAlchemy>=2.0.23 \
    python-dotenv>=1.0.0 \
    yfinance>=0.2.31 \
    requests>=2.31.0 \
    pytest>=7.4.3 \
    black>=23.11.0 \
    mypy>=1.7.0 \
    isort>=5.12.0 \
    jupyter>=1.0.0

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Start a bash shell by default
CMD ["/bin/bash"]
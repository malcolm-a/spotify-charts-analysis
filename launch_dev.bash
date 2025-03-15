#!/bin/bash

if command -v python > /dev/null 2>&1; then
    PYTHON_CMD="python"
elif command -v python3 > /dev/null 2>&1; then
    PYTHON_CMD="python3"
else
    echo "Python is not installed on this system."
    exit 1
fi

echo "Using $PYTHON_CMD"
$PYTHON_CMD --version

echo "Creating virtual environment..."
$PYTHON_CMD -m venv .venv

echo "Activating virtual environment..."
source .venv/bin/activate
echo "Python path: $(which python)"

echo "Installing required dependencies..."
pip install -r requirements.txt

echo "Starting services..."
if ! docker compose up -d; then
    echo "Failed to start services with Docker Compose."
    exit 1
fi

exec $SHELL
#!/bin/bash

# Load environment variables from .env file if it exists
if [ -f .env ]; then
    echo "Loading environment variables from .env..."
    export $(grep -v '^#' .env | xargs)
else
    echo "Warning: .env file not found. Running with current environment variables."
fi

# Run the sports price bot. If no arguments are passed, default to --config config.example
if [ $# -eq 0 ]; then
    python sports_price_bot.py --config config.example
else
    python sports_price_bot.py "$@"
fi

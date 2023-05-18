#!/bin/bash
set -e

export PORT=${PORT:-8501}

echo "Running streamlit app"
streamlit run user_interface.py --server.port $PORT

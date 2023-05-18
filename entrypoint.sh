#!/bin/bash
set -e

echo "Running streamlit app"
streamlit run user_interface.py --server.port 80

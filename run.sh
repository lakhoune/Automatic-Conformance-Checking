#!/bin/bash
nginx -t &&
service nginx start &&
streamlit run user_interface.py --theme.base "dark"
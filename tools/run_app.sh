#!/usr/bin/env zsh
# Complete setup and run script for unified_ingestor
# Creates venv, installs deps (with psycopg3), and runs the app

cd "/Users/nemasis/Library/CloudStorage/OneDrive-Personal/Desktop/Techcore code files"
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r "unified_ingestor/requirements.txt"
echo "Starting unified_ingestor..."
python -m unified_ingestor.main

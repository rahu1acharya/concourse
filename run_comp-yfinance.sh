#!/bin/sh
chmod +x py-script/run_comp-yfinance.sh
# Install dependencies from requirements.txt
pip install -r py-script/yfrequirements.txt

# Navigate to the directory containing scrape.py
cd py-script

# Run the scrape.py script
python comp-yfinanceapi.py

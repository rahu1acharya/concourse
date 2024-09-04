#!/bin/sh
chmod +x py-script/run_comp-bs.sh
# Install dependencies from requirements.txt
pip install -r py-script/requirements.txt

# Display the contents of secrets.env
echo "Contents of secrets.env:"
cat secrets-output/secrets.env

# Source environment variables
set -a
. secrets-output/secrets.env
set +a

# Navigate to the directory containing scrape.py
cd py-script

# Run the scrape.py script
python comp-bs.py

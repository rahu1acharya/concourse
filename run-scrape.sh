#!/bin/sh
chmod +x py-script/run-scrape.sh
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
python scrape-kgvc.py

# Move the output CSV file to the scraped-data directory
mv reliance_data2.csv ../scraped-data/

# Print the contents of the CSV file
echo "Contents of reliance_data2.csv in scraped-data directory:"
cat ../scraped-data/reliance_data2.csv

#!/bin/bash

# Create data directory if it doesn't exist
mkdir -p ./data

# Loop through months 01-12
for month in {01..12}; do
    file_path="./data/yellow_tripdata_2010-${month}.parquet"
    
    if [ -f "$file_path" ]; then
        echo "File for 2010-${month} already exists, skipping..."
    else
        echo "Downloading data for 2010-${month}..."
        curl -o "$file_path" \
             "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2010-${month}.parquet"
             
        
        # Add a small delay between downloads
        sleep 1
    fi
done

echo "Download complete!" 
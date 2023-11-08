import requests
import csv
from datetime import datetime

# Define the API endpoint URL
api_url = "http://localhost:8080/api/value/"

# Define the CSV data
csv_data = """time,station,FFX,LT2,PPX
2023-11-06T00:00+00:00,16400,97.0,0.4,960.9
"""

# Parse the CSV data and send it to the API
lines = csv_data.strip().split('\n')
header = lines[0].split(',')
data = [line.split(',') for line in lines[1:]]

for row in data:
    timestamp = datetime.fromisoformat(row[0])
    station = int(row[1])
    ffx = float(row[2])
    lt2 = float(row[3])
    ppx = float(row[4])

    payload = {
        "time": timestamp,
        "value_type_id": station,  # Assuming "station" corresponds to "value_type_id"
        "value": ffx  # Change this to the appropriate value field
    }

    response = requests.post(api_url, json=payload)

    if response.status_code == 200:
        print("Value added successfully!")
    else:
        print(f"Failed to add value. Status code: {response.status_code}")

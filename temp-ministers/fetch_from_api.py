import requests
import json

# API endpoint
url = 'https://handbookapi.aph.gov.au/api/ministryrecords/'

# Send GEt request
response  = requests.get(url)
data = response.json()

# Check if the request is successful
if response.status_code == 200:
    with open("data.json", "w") as f:
        json.dump(data, f)

    actual_data = data.get('value', [])
    print(actual_data)
else:
    print('Failed to retrieve data:', response.status_code)
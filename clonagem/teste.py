import requests
import json

url = "https://api.mercadolibre.com/items"

payload = json.dumps()
headers = {
  'Content-Type': 'application/json',
  'Authorization': 'Bearer APP_USR-1957800741627300-072614-560bbf821e8554fdf09cc5be3ad7eb26-538917322'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)

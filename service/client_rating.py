import requests

url = "http://127.0.0.1:8000/add_rating"
data = {
    "title_id": "tt0330429",
    "rate": 5
}

response = requests.post(url, json=data)

print(response.json())
import requests

url = "http://127.0.0.1:8000/query"
data = {
    "query": "MATCH (n) RETURN n LIMIT 5"
}

response = requests.post(url, json=data)

print(response.json())
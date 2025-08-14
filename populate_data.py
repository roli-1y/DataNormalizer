import requests
import json

BASE_URL = "http://127.0.0.1:5000/machines"

# Sample data
team_a_data = [
    {"os": "Ubuntu 20.04", "cpu_model": "Xeon E5", "memory_gb": 64},
    {"os": "Ubuntu 22.04", "cpu_model": "Xeon Gold", "memory_gb": "128"}
]

team_b_data = {
    "OperatingSystem": "Debian 12",
    "CPU": "Intel i7",
    "RAM": "16 GB"
}

team_c_data = {
    "OSName": "Windows Server 2022",
    "processor": ["Xeon Platinum", "Xeon Platinum"],
    "mem": 262144
}

# Headers for each team
headers = {
    "team_a": {"X-Source": "team_a", "Content-Type": "application/json"},
    "team_b": {"X-Source": "team_b", "Content-Type": "application/json"},
    "team_c": {"X-Source": "team_c", "Content-Type": "application/json"}
}

def post_data(data, team):
    """Send POST request to /machines endpoint."""
    try:
        response = requests.post(BASE_URL, headers=headers[team], json=data)
        print(f"POST /machines ({team}) response: {response.status_code} - {response.json()}")
    except requests.RequestException as e:
        print(f"Error sending data for {team}: {e}")

# Send sample data
post_data(team_a_data, "team_a")
post_data(team_b_data, "team_b")
post_data(team_c_data, "team_c")

# Verify data with GET /machines
try:
    response = requests.get(BASE_URL)
    print(f"GET /machines response: {response.status_code} - {json.dumps(response.json(), indent=2)}")
except requests.RequestException as e:
    print(f"Error fetching machines: {e}")

# Verify stats with GET /stats
try:
    response = requests.get("http://127.0.0.1:5000/stats")
    print(f"GET /stats response: {response.status_code} - {json.dumps(response.json(), indent=2)}")
except requests.RequestException as e:
    print(f"Error fetching stats: {e}")
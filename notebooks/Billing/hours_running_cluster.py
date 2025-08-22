import requests
import json
import dbutils
from datetime import datetime

# Accessing file through adls2 access key
storage_account = "devcmamqc"
account_conn = f"fs.azure.account.key.{storage_account}.dfs.core.windows.net"

# Databricks credentials
adb_scope = "devcmamqc-scope"
adb_key = "DBToken"
storage_account_key = dbutils.secrets.get(scope=adb_scope, key=adb_key)

# Databricks workspace information
workspace_url = os.getenv('DATABRICKS_WORKSPACE_URL')
token = storage_account_key

# Headers for authentication
headers = {
    "Authorization": f"Bearer {token}"
}

# Function to get cluster events
def get_cluster_events(cluster_id, start_time, end_time):
    url = f"{workspace_url}/api/2.0/clusters/events"
    payload = {
        "cluster_id": cluster_id,
        "start_time": start_time,
        "end_time": end_time,
        "order": "ASC",
        "event_types": ["start", "terminate"]
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code != 200:
        print(f"Error fetching events for cluster {cluster_id}: {response.text}")
        return None
    return response.json()

# Function to calculate total uptime
def calculate_uptime(events):
    if "events" not in events:
        print("No events found in the response")
        return 0
    total_uptime = 0
    start_time = None
    for event in events["events"]:
        if event["type"] == "start":
            start_time = event["timestamp"]
        elif event["type"] == "terminate" and start_time:
            end_time = event["timestamp"]
            total_uptime += (end_time - start_time) / 1000  # Convert milliseconds to seconds
            start_time = None
    return total_uptime / 3600  # Convert seconds to hours

# Define the time range for last month
start_time = int(datetime(2025, 6, 1).timestamp() * 1000)  # Replace with the first day of last month
end_time = int(datetime(2025, 7, 2, 23, 59, 59).timestamp() * 1000)  # Replace with the last day of last month

# Get the list of clusters
clusters_url = f"{workspace_url}/api/2.0/clusters/list"
clusters_response = requests.get(clusters_url, headers=headers)
clusters = clusters_response.json()["clusters"]

# Calculate uptime for each cluster
total_uptime = 0
for cluster in clusters:
    cluster_id = cluster["cluster_id"]
    events = get_cluster_events(cluster_id, start_time, end_time)
    if events:
        uptime = calculate_uptime(events)
        total_uptime += uptime

print(f"Total cluster uptime last month: {total_uptime} hours")
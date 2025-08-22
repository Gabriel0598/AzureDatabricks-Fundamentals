import requests
from requests.auth import HTTPBasicAuth
import json
from datetime import datetime

# Replace with your Azure Databricks workspace URL and personal access token
workspace_url = os.getenv('DATABRICKS_WORKSPACE_URL')
personal_access_token = os.getenv('DATABRICKS_TOKEN')

# Define the endpoint for the clusters API
endpoint = f"{workspace_url}/api/2.0/clusters/list"

# Set the headers
headers = {
    "Authorization": f"Bearer {personal_access_token}",
    "Content-Type": "application/json"
}

# Make the API request
response = requests.get(endpoint, headers=headers)

# Check if the request was successful
if response.status_code == 200:
    clusters = response.json().get('clusters', [])
    for cluster in clusters:
        cluster_id = cluster['cluster_id']
        cluster_name = cluster['cluster_name']
        start_time = datetime.fromtimestamp(cluster['start_time'] / 1000)
        current_time = datetime.now()
        running_time = current_time - start_time
        print(f"Cluster ID: {cluster_id}, Cluster Name: {cluster_name}, Running Time: {running_time}")
else:
    print(f"Failed to retrieve cluster data: {response.status_code} - {response.text}")
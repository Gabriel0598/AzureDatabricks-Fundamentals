import os
import json
import requests
from requests.auth import HTTPBasicAuth

# Replace with your Azure Databricks workspace URL and personal access token
workspace_url = os.getenv('DATABRICKS_WORKSPACE_URL')
personal_access_token = os.getenv('DATABRICKS_TOKEN')
account_id = os.getenv('DATABRICKS_ACCOUNT_ID')

# Define the endpoint for the usage API
endpoint = f"{workspace_url}/api/2.0/accounts/{account_id}/usage"

# Make the API request
response = requests.get(endpoint, auth=HTTPBasicAuth('token', personal_access_token))

# Check if the request was successful
if response.status_code == 200:
    usage_data = response.json()
    # Process the usage data as needed
    print(json.dumps(usage_data, indent=4))
else:
    print(f"Failed to retrieve usage data: {response.status_code} - {response.text}")
    
#-----------------------------------------------------------------------------------#
import requests
import json

# Replace with your Azure subscription ID and personal access token
subscription_id = os.getenv('AZURE_SUBSCRIPTION_ID')
personal_access_token = os.getenv('AZURE_PERSONAL_ACCESS_TOKEN')

# Define the endpoint for the usage API
endpoint = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Consumption/usageDetails?api-version=2021-10-01"

# Set the headers
headers = {
    "Authorization": f"Bearer {personal_access_token}",
    "Content-Type": "application/json"
}

# Make the API request
response = requests.get(endpoint, headers=headers)

# Check if the request was successful
if response.status_code == 200:
    usage_data = response.json()
    # Process the usage data as needed
    print(json.dumps(usage_data, indent=4))
else:
    print(f"Failed to retrieve usage data: {response.status_code} - {response.text}")
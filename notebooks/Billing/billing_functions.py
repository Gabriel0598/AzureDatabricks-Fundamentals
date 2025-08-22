import os
import requests
import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

def get_databricks_client():
    """
    Get Databricks client using environment variables or Azure Key Vault
    """
    # Method 1: Using environment variables (recommended for local development)
    if os.getenv('DATABRICKS_HOST') and os.getenv('DATABRICKS_TOKEN'):
        return WorkspaceClient(
            host=os.getenv('DATABRICKS_HOST'),
            token=os.getenv('DATABRICKS_TOKEN')
        )
    
    # Method 2: Using Azure CLI authentication (recommended for production)
    try:
        return WorkspaceClient()  # Will use Azure CLI or managed identity
    except Exception as e:
        print(f"Authentication failed: {e}")
        raise

def get_billing_usage():
    """
    Retrieve billing usage data from Databricks
    """
    client = get_databricks_client()
    
    # Use the system billing tables instead of REST API when possible
    try:
        # This assumes you have access to system.billing.usage table
        query = """
        SELECT billing_origin_product,
               usage_date,
               sum(usage_quantity) as usage_quantity
        FROM system.billing.usage
        WHERE month(usage_date) = month(NOW())
          AND year(usage_date) = year(NOW())
        GROUP BY billing_origin_product, usage_date
        """
        
        # Execute using Databricks SQL
        result = client.statement_execution.execute_statement(
            warehouse_id=os.getenv('DATABRICKS_WAREHOUSE_ID'),
            statement=query
        )
        
        return result
        
    except Exception as e:
        print(f"Error retrieving billing data: {e}")
        raise

if __name__ == "__main__":
    billing_data = get_billing_usage()
    print(json.dumps(billing_data, indent=2))
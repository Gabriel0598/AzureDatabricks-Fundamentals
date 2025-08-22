import os
import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterDetails

def get_databricks_client():
    """
    Get authenticated Databricks client using secure methods
    """
    # Use environment variables or Azure authentication
    if os.getenv('DATABRICKS_HOST'):
        return WorkspaceClient(
            host=os.getenv('DATABRICKS_HOST'),
            # Token will be automatically retrieved from environment or Azure CLI
        )
    else:
        # Use default authentication (Azure CLI, managed identity, etc.)
        return WorkspaceClient()

def get_cluster_information():
    """
    Retrieve cluster information from Databricks workspace
    """
    client = get_databricks_client()
    
    try:
        clusters = list(client.clusters.list())
        
        cluster_info = []
        for cluster in clusters:
            info = {
                "cluster_id": cluster.cluster_id,
                "cluster_name": cluster.cluster_name,
                "state": cluster.state.value if cluster.state else "Unknown",
                "node_type_id": cluster.node_type_id,
                "num_workers": cluster.num_workers,
                "driver_node_type_id": cluster.driver_node_type_id,
                "spark_version": cluster.spark_version,
                "autotermination_minutes": cluster.autotermination_minutes
            }
            cluster_info.append(info)
            
        return cluster_info
        
    except Exception as e:
        print(f"Error retrieving cluster information: {e}")
        raise

if __name__ == "__main__":
    clusters = get_cluster_information()
    print(json.dumps(clusters, indent=2))
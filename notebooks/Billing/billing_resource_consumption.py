%python
# Import necessary libraries
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("BillingUsageExample").getOrCreate()

# Load the system.billing.usage table
billing_usage_df = spark.sql("SELECT * FROM system.billing.usage")

# Load the system.lakeflow.jobs table
jobs_df = spark.sql("SELECT * FROM system.lakeflow.jobs")

# Enrich usage data with job names
enriched_usage_df = billing_usage_df.alias("usage").join(
    jobs_df.alias("jobs"),
    (billing_usage_df.workspace_id == jobs_df.workspace_id) & 
    (billing_usage_df.usage_metadata.job_id == jobs_df.job_id),
    "left"
).select(
    "usage.*",
    "jobs.name as job_name"
)

# Filter for a specific job ID (replace 'your_job_id' with the actual job ID)
filtered_usage_df = enriched_usage_df.filter(enriched_usage_df.usage_metadata.job_id == 'your_job_id')

# Show the filtered usage data
filtered_usage_df.show()

# Calculate total consumption and cost for the specific job
total_consumption = filtered_usage_df.groupBy("usage_metadata.job_id").sum("usage_quantity").collect()[0][1]
total_cost = filtered_usage_df.groupBy("usage_metadata.job_id").sum("cost").collect()[0][1]

print(f"Total Consumption for Job ID 'your_job_id': {total_consumption}")
print(f"Total Cost for Job ID 'your_job_id': {total_cost}")
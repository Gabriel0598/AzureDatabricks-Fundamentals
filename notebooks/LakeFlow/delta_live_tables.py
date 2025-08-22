import dlt
from pyspark.sql.functions import *

import os

os.environ["UNITY_CATALOG_VOLUME_PATH"] = "/Volumes/<catalog-name>/<schema-name>/<volume-name>/"
os.environ["DATASET_DOWNLOAD_URL"] = "https://health.data.ny.gov/api/views/jxy9-yhdk/rows.csv"
os.environ["DATASET_DOWNLOAD_FILENAME"] = "rows.csv"

dbutils.fs.cp(f"{os.environ.get('DATASET_DOWNLOAD_URL')}", f"{os.environ.get('UNITY_CATALOG_VOLUME_PATH')}{os.environ.get('DATASET_DOWNLOAD_FILENAME')}")

@dlt.table(
  comment="Popular baby first names in New York. This data was ingested from the New York State Department of Health."
)
def baby_names_raw():
  df = spark.read.csv(f"{os.environ.get('UNITY_CATALOG_VOLUME_PATH')}{os.environ.get('DATASET_DOWNLOAD_FILENAME')}", header=True, inferSchema=True)
  df_renamed_column = df.withColumnRenamed("First Name", "First_Name")
  return df_renamed_column

@dlt.table(
  comment="New York popular baby first name data cleaned and prepared for analysis."
)
@dlt.expect("valid_first_name", "First_Name IS NOT NULL")
@dlt.expect_or_fail("valid_count", "Count > 0")
def baby_names_prepared():
  return (
    dlt.read("baby_names_raw")
      .withColumnRenamed("Year", "Year_Of_Birth")
      .select("Year_Of_Birth", "First_Name", "Count")
  )

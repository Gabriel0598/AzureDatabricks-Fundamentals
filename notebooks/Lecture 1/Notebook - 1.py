# Databricks notebook source
"This is my first notebook"

# COMMAND ----------

sum = 2 + 3
print(sum)

# COMMAND ----------

sc

# COMMAND ----------

# MAGIC %sh ls

# COMMAND ----------

# MAGIC %sh ls -al

# COMMAND ----------

# MAGIC %sh mkdir logs/shan

# COMMAND ----------



# COMMAND ----------

# MAGIC %sh ls -al

# COMMAND ----------

cd logs

# COMMAND ----------

# MAGIC %sh ls -al

# COMMAND ----------

cd ..

# COMMAND ----------

ll

# COMMAND ----------

# MAGIC %md #This is my first notebook

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

containername = "customcontainer"
storagename = "azuredb09"
mountpoint =  "/mnt/custommount"
sas = "f/18xGBt5riB9e5nHNSQ+NnPwKwbSSHT6e123akow8vN3f0A3vD3AV3SNJtfNB+IOA5hOuahqoK0+ASt+lODEQ=="

try:
    dbutils.fs.mount(source = "wasbs://" + containername + "@" + storagename + ".blob.core.windows.net",
                mount_point = mountpoint,
                extra_configs = {"fs.azure.account.key." + storagename + '.blob.core.windows.net': sas})

except Exception as e:
    print("Already mounted, please unmount using dbutils.fs.unmount")

# COMMAND ----------

if any(mount.mountPoint == mountpoint for mount in dbutils.fs.mounts()):    dbutils.fs.unmount("/mnt/custommount")

# COMMAND ----------

# MAGIC %fs head /mnt/custommount/trans_last_24h_pix_trat_2022-03-18.csv

# COMMAND ----------

# MAGIC %scala
# MAGIC val mydataframe = spark.read.option("header", "true").option("inferSchema", "true").csv("/mnt/custommount")
# MAGIC display(mydataframe)

# COMMAND ----------

# MAGIC %scala
# MAGIC val selectexpression = mydataframe.select("QuantidadeMedia", "TotalMedio")
# MAGIC display(selectexpression)

# COMMAND ----------

# MAGIC %scala
# MAGIC val renameddata = selectexpression.withColumnRenamed("QuantidadeMedia", "Quantidade_Geral")
# MAGIC display (renameddata)

# COMMAND ----------

# MAGIC %scala
# MAGIC val mydataframe = spark.read.option("header", "true").option("inferSchema", "true").csv("/mnt/custommount")
# MAGIC val selectexpression = mydataframe.select("QuantidadeMedia", "TotalMedio")
# MAGIC val renameddata = selectexpression.withColumnRenamed("QuantidadeMedia", "Quantidade_Geral")
# MAGIC //display(renameddata.describe("Quantidade_Geral", "TotalMedio"))
# MAGIC //val filter = renameddata.where("TotalMedio > 100")
# MAGIC //display(filter)
# MAGIC display(renameddata)
# MAGIC renameddata.createOrReplaceTempView("PIXdiarydata")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM PIXdiarydata WHERE TotalMedio = '19518888.7';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(Quantidade_Geral) FROM PIXdiarydata GROUP BY Quantidade_Geral ORDER BY SUM(TotalMedio);

# COMMAND ----------

# MAGIC %scala
# MAGIC val aggregatedata = spark.sql("""SELECT SUM(Quantidade_Geral) FROM PIXdiarydata GROUP BY Quantidade_Geral ORDER BY SUM(TotalMedio);""")
# MAGIC aggregatedata.write.option("header", "true").format("com.databricks.spark.csv").save("/mnt/custommount.output/trans_day_pix.csv")

# COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, IntegerType, StringType, BooleanType, FloatType}

var fireschema = StructType(Array(StructField("CallNumber", IntegerType, true),
                                 StructField("UnitID", StringType, true),
                                 StructField("IncidentNumber", IntegerType, true),
                                 StructField("CallType", StringType, true),
                                 StructField("CallDate", StringType, true),
                                 StructField("WatchDate", StringType, true),
                                 StructField("CallFinalDisposition", StringType, true),
                                 StructField("AvailableDtTm", StringType, true),
                                 StructField("Address", StringType, true),
                                 StructField("City", StringType, true),
                                 StructField("ZipCode", IntegerType, true),
                                 StructField("Battalion", StringType, true),
                                 StructField("StationArea", StringType, true),
                                 StructField("Box", StringType, true),
                                 StructField("OriginalPriority", StringType, true),
                                 StructField("Priority", StringType, true),
                                 StructField("FinalPriority", IntegerType, true),
                                 StructField("ALSUnit", BooleanType, true),
                                 StructField("CallTypeGroup", StringType, true),
                                 StructField("NumAlarms", IntegerType, true),
                                 StructField("UnitType", StringType, true),
                                 StructField("UnitSequenceInCallDispatch", IntegerType, true),
                                 StructField("FirePreventionDistrict", StringType, true),
                                 StructField("SupervisorDistrict", StringType, true),
                                 StructField("Neighborhood", StringType, true),
                                 StructField("Location", StringType, true),
                                 StructField("RowID", StringType, true),
                                 StructField("Delay", FloatType, true),)
                            
val FireFile = "/FileStore/tables/firecsv/Fire_Incidents.csv"
val fireDF = spark.read.schema(fireschema).option("header","true").csv(FireFile)
display(fireDF)

# COMMAND ----------

val fewFireDF = fireDF.select("IncidentNumber", "AvailableDtTm", "CallType").where($"CallType" =!= "Medical Incident")
fewFireDF.show(10)

# COMMAND ----------

import org.apache.spark.sql.functions._


fireDF.select("CallType").where(col("CallType").isNotNull).agg(countDistinct('CallType') as 'DistincCallTypes').show()

# COMMAND ----------

fireDF.select("CallType").where(col("CallType").isNotNull).groupBy("CallType").count().orderBy(desc("count")).show(5)

# COMMAND ----------

val newFireDF = fireDF.withcolumnRenamed("Delay", "ResponseDelayedinMins")
newFireDF.select("ResponseDelayedMins").where($"ResponseDelayedMins" < 5).show(10)

# COMMAND ----------

import org.apache.spark.sql.{functions => F}
newFireDF.select(F.sum("NumAlarms"), F.avg("ResponseDelayedMins"),
                F.min("ResponseDelayedMins"), F.max("ResponseDelayedMins")).show()

# COMMAND ----------

val fewFireDF = fireDF.select("IncidentNumber", "AvailableDtTm", "CallType").where($"CallType" =!= "Mecial Incident")
// fewFireDF.show(10)

import org.apache.spark.sql.functions._

fireDF.select("CallType").where(col("CallType").isNotNull).agg(countDistinct('CallType') as 'DistinctCallTypes')
// .show()

fireDF.select("CallType").where(col("CallType").isNotNull).groupBy("CallType").count().orderBy(desc("count"))
// .show(5)

val newFireDF - fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")
newFireDF.select("ResponsibleDelayedMins").where($"ResponseDelayedMins" < 5)
// .show(10)

import org.apache.spark.sql.{functions => F}
newFireDF.select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
                F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
// .show()

val datastream = newFireDF.writeStream.format("console").outputMode("append").option("checkpointLocation", "dbfs:/mnt").start()

# COMMAND ----------


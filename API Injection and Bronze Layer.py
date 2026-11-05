# Databricks notebook source
# DBTITLE 1,Import Req Library
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# DBTITLE 1,Create CataLog, Schema and Volume
spark.sql("CREATE CATALOG IF NOT EXISTS workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.default")
spark.sql("create volume if not exists workspace.default.cricket_api_project")

base_path = '/Volumes/workspace/default/cricket_api_project/'


# COMMAND ----------

# DBTITLE 1,Calling API
API_KEY = 'e0503f22-bfde-4087-9035-f93f9d974fa6'
api_url = f"https://api.cricapi.com/v1/countries?apikey=e0503f22-bfde-4087-9035-f93f9d974fa6&offset=0"

response = requests.get(api_url)
response.raise_for_status()

api_data = response.json()

print(api_data.keys())
print(json.dumps(api_data, indent=4)[:2000])

# COMMAND ----------

# DBTITLE 1,Saving The API response in the volume
import json
raw_data_file = f'{base_path}/current_matches_raw.json'

with open(raw_data_file,'w') as file:
    json.dump(api_data,file)

print("Data has been uploaded to the file at:",raw_data_file)

# COMMAND ----------

# DBTITLE 1,Create Bronze layer DataFrame
bronze_data = [{
    "source_api" : api_url,
    "raw_json": json.dumps(api_data),
    "injection_time":None,
}]

bronze_schema = StructType([
    StructField("source_api",StringType(),True),
    StructField("raw_json",StringType(),True),
    StructField("injection_time",TimestampType(),True)
])

bronze_df = spark.createDataFrame(bronze_data,schema=bronze_schema)\

bronze_df.display()

# COMMAND ----------

# DBTITLE 1,Saving the Data In Bronze Table
bronze_df.write\
    .format('delta')\
    .mode('overwrite')\
    .saveAsTable("workspace.default.cricket_bronze_current_matches")

print("Bronze table created Successfully")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from workspace.default.cricket_bronze_current_matches
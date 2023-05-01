# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Simplify ETL with Delta Live Table && MetaData
# MAGIC
# MAGIC DLT makes Data Engineering accessible for all. Just declare your transformations in SQL or Python, and DLT will handle the Data Engineering complexity for you.
# MAGIC
# MAGIC **Accelerate ETL development** <br/>
# MAGIC Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance in a declaritive fashion
# MAGIC
# MAGIC **Remove operational complexity** <br/>
# MAGIC By automating complex administrative tasks and gaining broader visibility into pipeline operations
# MAGIC
# MAGIC **Trust your data** <br/>
# MAGIC With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
# MAGIC
# MAGIC **Simplify batch and streaming** <br/>
# MAGIC With self-optimization and auto-scaling data pipelines for batch or streaming processing 
# MAGIC
# MAGIC ## Our Delta Live Table pipeline
# MAGIC
# MAGIC We pre-processed our raw XML into bronze layer- in silver layer we'll be leveraging DLT's declarative framework to manage dependencies and metadata logic to produce several silver tables from single snippet of code

# COMMAND ----------

#setup Environment and Libraries

import dlt
import ast
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Below is a sample metadata table stored on delta. The repo contains sample json file with metadata info written out. This provides an programatic methodology to handle several different table and data types in by decoupleing code from the actual data. The function will leverage the source_name field in the actual data - and based on that source_name field - provide custom logic to process data - including providing table_name, extracting intersting columns, and applying unique expectations. 
# MAGIC
# MAGIC SELECT * FROM jmr_dlt.metadata_xml

# COMMAND ----------

# This function takes metadata variables (m) - and then leverages Delta Live Tables  APIs to create tables in support our pipeline. By passing unique variables - DLT will create a pipeline with correct dependancies, columns, and expectations driveng by this simple code snippet.


def table_iterator(mcolumns, mexpectations, mtable, msource,mcomment):

  @dlt.create_table(name=f"{mtable}", comment=f"{mcomment}")
  @dlt.expect_all_or_drop(mexpectations)

  def silverTables():
    return (dlt.read(name="write2bronze")
          .filter(col("type")==f"{msource}")
          .withColumn('type', regexp_replace('type', 'HKQuantityTypeIdentifier|HKCategoryTypeIdentifier', ''))
          .withColumn("timestamp",to_timestamp(date_format(from_utc_timestamp(col("startDate"),'America/New_York'),'yyyy-MM-dd HH:mm:ss')))
          .withColumn("value",col('value').cast(DoubleType()))
          .withColumn("week",weekofyear(col('timestamp')))
          .withColumn("day",dayofweek(col('timestamp')))
          .withColumn("dayOfWeek", date_format('timestamp', 'E'))
          .withColumn("hourOfDay", date_format('timestamp', 'H'))
          .withColumn('dateMinute',to_timestamp(date_trunc("minute",col('timestamp')),'MM-dd-yyyy HH:mm'))
          .withColumn("dateHour",to_timestamp(date_trunc("hour",col('timestamp')),'MM-dd-yyyy HH:mm'))
          .withColumn("dateDay",to_timestamp(date_trunc("DAY",col('timestamp')),'MM-dd-yyyy'))
          .select(*mcolumns)
         )

# COMMAND ----------

# This Cell is the driver for our Metadata Function - takes in metadata delta table with unique variables to then pass to function. 

metadata_table=spark.table("jmr_dlt.metadata_xml")

sourceList=(metadata_table
           .select('source_name')
           .rdd.map(lambda x : x[0]).collect())



for source in sourceList:
  metadata=metadata_table.filter(col('source_name') == f"{source}").first()

  msource=metadata.source_name
  mtable=metadata.table_name
  mcomment=metadata.comments
  x= ast.literal_eval(metadata.columns) #converting string to list
  mcolumns= [n.strip() for n in x]
  mexpectations=json.loads(metadata.expectations)
  
  table_iterator(mcolumns, mexpectations, mtable, msource,mcomment)

# Databricks notebook source
# MAGIC %md ### Metadata FTW!

# COMMAND ----------

#setup Environment and Libraries

import dlt
import ast
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM jmr_dlt.metadata_xml

# COMMAND ----------


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

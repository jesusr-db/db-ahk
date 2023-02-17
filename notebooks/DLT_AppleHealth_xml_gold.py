# Databricks notebook source
# MAGIC %pip install mlflow

# COMMAND ----------

#setup Environment and Libraries

import dlt
import mlflow
import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import Row
from pyspark.ml.feature import Imputer

# COMMAND ----------

@dlt.table(
  name="kCal_union",
  comment="union All energy Silver tables"
)

@dlt.expect_or_drop("start_time_valid", "timestamp IS NOT NULL")
 
def workout_union():
  workoutUnion=[]
  workoutList=(spark
              .table("jmr_dlt.metadata_xml")
              .filter(col("table_name").contains("EnergyBurned"))
              .select(col('table_name'))
              .rdd.map(lambda x : x[0]).collect()
             )
  for t in workoutList:
    workoutUnion.append(f"{t}")
    
    
  target_tables = [dlt.read(t) for t in workoutUnion]
  unioned = functools.reduce(lambda x,y: x.union(y), target_tables)
  return (
    unioned.withColumn("kcal",col('value')).select(col("*"))
  )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Silver layer: joining tables while ensuring data quality
# MAGIC 
# MAGIC 
# MAGIC Once the bronze layer is defined, we'll create the sliver layers by Joining data. Note that bronze tables are referenced using the `LIVE` namespace. 
# MAGIC 
# MAGIC To consume only increment from the Bronze layer like `raw_txs`, we'll be using the `stream` keyword: `stream(LIVE.raw_txs)`
# MAGIC 
# MAGIC Note that we don't have to worry about compactions, DLT handles that for us.
# MAGIC 
# MAGIC #### Expectations
# MAGIC 
# MAGIC DLT currently supports three modes for expectations:
# MAGIC 
# MAGIC | Mode | Behavior |
# MAGIC | ---- | -------- |
# MAGIC | `EXPECT(criteria)` in SQL or `@dlt.expect` in Python  | Record metrics for percentage of records that violate expectation <br> (**NOTE**: this metric is reported for all execution modes) |
# MAGIC | `EXPECT (criteria) ON VIOLATION FAIL UPDATE` in SQL or `@dlt.expect_or_fail` in Python| Fail the pipeline when expectation is not met |
# MAGIC | `EXPECT (criteria) ON VIOLATION DROP ROW` in SQL or `@dlt.expect_or_drop` in Python| Only process records that fulfill expectations |
# MAGIC 
# MAGIC By defining expectations (`CONSTRAINT <name> EXPECT <condition>`), you can enforce and track your data quality. See the [documentation](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-expectations.html) for more details

# COMMAND ----------

# goldHR dailyAgg

@dlt.table(name='dailyAgg_hr_gold',comment='avg, min, max HR by day')
@dlt.expect_or_drop("valid HR", "avgBpm IS NOT NULL")

def dailyAgg_hr_gold():
  return(dlt.read(name="heartrate").withColumn('bpm',col('value')).groupBy(col('dateDay')).agg(round(min(col('bpm')),2).alias('restingBpm'),round(avg(col('bpm')),2).alias('avgBpm'),round(max(col('bpm')),2).alias('maxBpm')))

# COMMAND ----------

# silver heartRate by Minute and Zone,bpmRange 

@dlt.table(name='MinuteAgg_HRzone_gold',comment='calc HR zone by minute')
@dlt.expect_or_drop("valid HR", "avgBpm IS NOT NULL")

def MinuteAgg_HRzone_gold():
  
  return(dlt
      .read(name='heartrate').withColumn('bpm',col('value'))
      .groupBy(col('dateMinute')).agg(round(min(col('bpm')),2).alias('minBpm'),round(avg(col('bpm')),2).alias('avgBpm'),round(max(col('bpm')),2).alias('maxBpm'))
      .orderBy(col('dateMinute').desc())).withColumn('hrZone',when(col('avgBpm')<=lit(132),1)
                                                     .when((col('avgBpm')>=lit(132)) & (col('avgBpm')<=lit(143)) ,2)
                                                     .when((col('avgBpm')>=lit(143)) & (col('avgBpm')<=lit(155)) ,3)
                                                     .when((col('avgBpm')>=lit(155)) & (col('avgBpm')<=lit(166)) ,4)
                                                     .when((col('avgBpm')>=lit(166)),5)
                                                     .otherwise(lit('0'))).withColumn('bpmRange',col('maxBpm')-col('minBpm'))

# COMMAND ----------

@dlt.table(name='dailyAgg_mass_move_gold',comment='daily_mass_move_gold')
@dlt.expect_or_drop("valid HR", "avgBpm IS NOT NULL")

def daily_mass_move_gold():



  hr=(dlt.read(name="heartrate").withColumn('bpm',col('value'))
      .groupBy(col('dateDay'))
      .agg(round(min(col('bpm')),2).alias('restingBpm'),round(avg(col('bpm')),2).alias('avgBpm'),round(max(col('bpm')),2).alias('maxBpm'))
     )

  move=(dlt.read(name='ActiveEnergyBurned')
        .select(col('dateDay'),col('value'))).groupBy(col('dateDay')).agg(sum(col('value')).alias("dailyKcal"))
  
  
  mass=(dlt.read(name='bodymass').withColumn('weightlb',col('value')).withColumn("dateDay", to_date(col('dateDay'),'yyyy-MM-dd'))).groupBy(col('dateDay')).agg(avg(col('weightlb')).alias("weightlb"))
  
  joined = (hr.join(move,"dateDay","left")
                  .join(mass,"dateDay","left"))


# fill in nulls with avg when  missed a weightlb

impute_cols = [
    "weightlb"
]
imputer = Imputer(strategy="median", inputCols=impute_cols, outputCols=impute_cols)

imputer_model = imputer.fit(df)
joined = imputer_model.transform(joined)

# display(imputed_df.orderBy(col('dateDay').desc()))

# weight fluctuates tremendously from day to day - this will have rolling 14 day window average
  w = (Window.orderBy(col("dateDay")).rowsBetween(-7, 7))
  
  return(joined.withColumn('weightlbRolling', avg("weightlb").over(w)).dropna(subset=['avgBpm','dailyKcal']))

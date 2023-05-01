# Databricks notebook source
# MAGIC %pip install bamboolib

# COMMAND ----------

schema_name='jmr_ahk_test_1'

# COMMAND ----------

import pandas as pd
import numpy as np
import bamboolib as bam

df = spark.table(f'hive_metastore.{schema_name}.write2bronze').toPandas()

df

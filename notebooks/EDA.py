# Databricks notebook source
# MAGIC %pip install bamboolib

# COMMAND ----------

import pandas as pd
import numpy as np
import bamboolib as bam

df = spark.table('hive_metastore.jmr_ahk_test.write2bronze').toPandas()


df

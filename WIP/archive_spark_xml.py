# Databricks notebook source
# MAGIC %pip install gdown

# COMMAND ----------



import gdown
import os
import zipfile
from pyspark.sql.functions import *
from pyspark.sql.types import *
import xml.etree.ElementTree as ET
import pandas as pd
import re

# gDown variables
url = "https://drive.google.com/file/d/1KHnXtK90djbzgnPDxU989JrcE9UxLgjJ/view?usp=sharing" #replace with Gdrive sharing link
output = "/tmp/export.zip"

# COMMAND ----------

#gDown files onto /tmp directory of filesystem
gdown.download(url=url, output=output, quiet=False, fuzzy=True)

# COMMAND ----------

with zipfile.ZipFile(output, 'r') as zip_ref:
  zip_ref.extractall(f'/tmp/')

# COMMAND ----------

# https://discussions.apple.com/thread/254202523?answerId=257895569022#257895569022

patch_script="""
--- export.xml  2022-09-18 15:17:09.000000000 -0400
+++ export-fixed.xml    2022-09-18 16:37:08.000000000 -0400
@@ -15,6 +15,7 @@
   HKCharacteristicTypeIdentifierBiologicalSex       CDATA #REQUIRED
   HKCharacteristicTypeIdentifierBloodType           CDATA #REQUIRED
   HKCharacteristicTypeIdentifierFitzpatrickSkinType CDATA #REQUIRED
+  HKCharacteristicTypeIdentifierCardioFitnessMedicationsUse CDATA #IMPLIED
 >
 <!ELEMENT Record ((MetadataEntry|HeartRateVariabilityMetadataList)*)>
 <!ATTLIST Record
@@ -39,7 +40,7 @@
   startDate     CDATA #REQUIRED
   endDate       CDATA #REQUIRED
 >
-<!ELEMENT Workout ((MetadataEntry|WorkoutEvent|WorkoutRoute)*)>
+<!ELEMENT Workout ((MetadataEntry|WorkoutEvent|WorkoutRoute|WorkoutStatistics)*)>
 <!ATTLIST Workout
   workoutActivityType   CDATA #REQUIRED
   duration              CDATA #IMPLIED
@@ -63,7 +64,7 @@
   duration             CDATA #IMPLIED
   durationUnit         CDATA #IMPLIED
 >
-<!ELEMENT WorkoutEvent EMPTY>
+<!ELEMENT WorkoutEvent (MetadataEntry?)>
 <!ATTLIST WorkoutEvent
   type                 CDATA #REQUIRED
   date                 CDATA #REQUIRED
@@ -79,6 +80,7 @@
   minimum              CDATA #IMPLIED
   maximum              CDATA #IMPLIED
   sum                  CDATA #IMPLIED
+  unit                 CDATA #IMPLIED
 >
 <!ELEMENT WorkoutRoute ((MetadataEntry|FileReference)*)>
 <!ATTLIST WorkoutRoute
@@ -153,6 +155,7 @@
   dateIssued       CDATA #REQUIRED
   expirationDate   CDATA #REQUIRED
   brand            CDATA #IMPLIED
+>
 <!ELEMENT RightEye EMPTY>
 <!ATTLIST RightEye
   sphere           CDATA #IMPLIED
@@ -203,13 +206,6 @@
   diameter         CDATA #IMPLIED
   diameterUnit     CDATA #IMPLIED
 >
-  device           CDATA #IMPLIED
-<!ELEMENT MetadataEntry EMPTY>
-<!ATTLIST MetadataEntry
-  key              CDATA #IMPLIED
-  value            CDATA #IMPLIED
->
->
 ]>
 <HealthData>
  <ExportDate/>
"""
dbutils.fs.put("file:/tmp/jesus.rodriguez@databricks.com/healthkit/apple_health_export/patch.txt", patch_script, True)

# COMMAND ----------

# execute patch of xml
os.system('cd /tmp/apple_health_export/ && patch < patch.txt')

# COMMAND ----------

dbutils.fs.cp('file:/tmp/apple_health_export/export.xml','/tmp/jesus.rodriguez@databricks.com/asdf/')

# COMMAND ----------

xmldata =(spark
          .read
          .format('xml')
#           .schema(schema)
#           .option("rootTag","HealthData")
          .option("rowTag","Record")
          .option("inferSchema","false")
          .option("valueTag","_stuff")
          .load("/tmp/jesus.rodriguez@databricks.com/asdf/")
         )
display(xmldata)

# COMMAND ----------

xmldata=xmldata.withColumn('_type', regexp_replace('_type', 'HKQuantityTypeIdentifier', ''))
display(xmldata)

# COMMAND ----------

from pyspark.sql.functions import *
display(xmldata
#         .filter(col('value')>= 27)
        .filter((col('_type')=="HeartRateVariabilitySDNN"))
        .select('*')
        .orderBy(col('_startDate').desc()))

# COMMAND ----------

sourcetype_df=xmldata.select(col('_type')).groupBy(col('_type')).agg(count(col('*')).alias('count')).orderBy(col('count').desc())
display(sourcetype_df)

# COMMAND ----------

# save as managed Table
xmldata.write.format('delta').partitionBy('_type').mode('overwrite').saveAsTable('uc_demos_jesus_rodriguez.health.hk_arch_bronze')
sourcetype_df.write.format('delta').mode('overwrite').saveAsTable('uc_demos_jesus_rodriguez.health.hk_arch_sourcetype')

# COMMAND ----------

metadata=spark.read.json('file:/Workspace/Repos/jesus.rodriguez@databricks.com/db-ahk/metadata.json')
display(metadata)

# Databricks notebook source
# MAGIC %pip install gdown

# COMMAND ----------

# MAGIC 
# MAGIC 
# MAGIC %md 
# MAGIC # Apple HealthKit for Databricks
# MAGIC 
# MAGIC Our bronze notebook is all about data acquisition and landing into our lakehouse. Please follow the process to export Apple HealthKit data on apple site (link). The result of this process is a zip file with several metrics - in our case, we'll upload this zip file to Google drive and then share link (link). There are several other sharing methods - pick whichever is easiest for you (download to icloud, email, etc..). 
# MAGIC 
# MAGIC Once in Google Drive - we'll download export.zip using 'gDown', unzip file on the OS Filesystem (not dbfs). If you are on a later version of iOS - you'll have to run this simple script in cell 6 & 7 to fix malformed XML. 
# MAGIC 
# MAGIC Final step, we'll parse XML using Elmementree - create dataframe, and ingest into DLT!

# COMMAND ----------

#setup Environment and Libraries

import dlt
import gdown
import os
# import re
# import json
# import ast
# import datetime
import zipfile
from pyspark.sql.functions import *
# from pyspark.sql.types import *
# from pyspark.sql.window import Window
# from pyspark.sql import Row
import xml.etree.ElementTree as ET
import pandas as pd


# gDown variables
url = "https://drive.google.com/file/d/xxxxxxx/view?usp=sharing" #replace with Gdrive sharing link
output = "/tmp/export.zip"

# COMMAND ----------

#gDown files onto /tmp directory of filesystem
gdown.download(url=url, output=output, quiet=False, fuzzy=True)

# COMMAND ----------

with zipfile.ZipFile(output, 'r') as zip_ref:
    zip_ref.extractall(f'/tmp/')

# COMMAND ----------

# if on latest version of IOS - the exported xml from apple is malformed which prevents proper parsing with elementree. The script in this cell will fix it. Reference below:
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
dbutils.fs.put("file:/tmp/apple_health_export/patch.txt", patch_script, True)

# COMMAND ----------

# execute patch of xml
os.system('cd /tmp/apple_health_export/ && patch < patch.txt')

# COMMAND ----------

# parse our fixed XML and start DLT!
tree = ET.parse('/tmp/apple_health_export/export.xml') 
root = tree.getroot()
record_list = [x.attrib for x in root.iter('Record')]
data = pd.DataFrame(record_list)

@dlt.table(comment="write2bronze")

def write2bronze():
  landing=spark.createDataFrame(data)
  return(landing)

# COMMAND ----------

@dlt.table(name='datatype',comment='distinct data types in current batch')
def datatype():
  return(dlt.read(name='write2bronze').select(col('type')).dropDuplicates())

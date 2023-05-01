# db-ahk? But Why?

Get healthy with Databricks and Apple healthKit. 

HealthKit provides a central repository for health and fitness data on iPhone and Apple Watch. 

HealtKit Tracks a multitude of health metrics natively (and many more thru 3rd party integrations) including
- Vo2Max
- Heart Rate (BPM)
- Heart Rate Zone
- Steps
- Weight
- And many more!

It all started with a question - I wonder if I can get more insights and actionable information on my health (than app) - by analyzing raw data?


## Try Databricks for Free

Sign up for a [free Databricks trial](https://databricks.com/try-databricks?itm_data=demos-try-workflows-trial) to explore this demo. 

# Delta-Live-Tables (DLT)

Welcome to the repository for the Databricks Delta Live Tables Apple HealthKit. 

This Delta Live Tables (DLT) demo was built to have a more realistic data engineering end-to end demo.  DLT is the first ETL framework that uses a simple declarative approach to building reliable data pipelines and automatically manages your infrastructure at scale. Data analysts and engineers spend less time on tooling and can focus on getting value from data. With DLT, engineers are able to treat their **data as code** and apply **modern software engineering best practices** like testing, error handling, monitoring, and documentation to deploy reliable pipelines at scale.

## dataflow of our pipeline

![This is an image](https://github.com/jesusr-db/db-ahk/blob/main/images/SkillBuilder_%20DLT_JMR.png)

## DLT pipeline Example
![This is an image](https://github.com/jesusr-db/db-ahk/blob/main/images/DLTPipeline.png)


## Setup/Requirements
- Follow Apple Support instructions on how to export Apple HealthKit data from health App.
- Bronze Notebook - For data acquisition - I've selected GoogleDrive and share link to download data. Bronze Notebook is setup to use this method. If you choose other export functions, YMMV.
- Silver Notebook - Metadata table is written on delta. For sample metadata - please use the metadata.json file included in repo. 
- When configuring metastore in DLT configuration - please select HiveMetastore
- Depending XML data volume in export file - you may need to increase size of driver to support XML parsing (see documentation)


## Running the demo 
- Use [Databricks Repos](https://docs.databricks.com/repos/index.html#clone-a-remote-git-repository) from your Databricks workspace to clone the repo and kick off the demo. The repo consists of the three notebooks listed above. The workflow definition is not part of the repo.
- Trigger the DLT pipeline. Note, that although the pipeline is defined in the notebooks, you have to [create a pipeline first](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-ui.html) to execute it.
- Explore Metrics!
![This is an image](https://github.com/jesusr-db/db-ahk/blob/main/images/DbSqlDash.png)

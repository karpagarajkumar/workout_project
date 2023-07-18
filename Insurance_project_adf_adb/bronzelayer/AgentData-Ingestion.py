# Databricks notebook source
# MAGIC %md
# MAGIC <b> Load the Agent data from landing folder to Dataframe with extra column 'merge_flag'. </b>
# MAGIC <br> Use the schema as: agent_id integer, agent_name string, agent_email string,agent_phone string, branch_id integer, create_timestamp timestamp
# MAGIC <br>
# MAGIC <b> Save it as Agent Table with location as bronzelayer/Agent

# COMMAND ----------

from pyspark.sql.functions import lit

agent_schema="agent_id integer, agent_name string, agent_email string,agent_phone string, branch_id integer, create_timestamp timestamp"

df=spark.read.parquet('/mnt/landing/Agent', schema=agent_schema)

df1=df.withColumn('merge_flag', lit(False)).show()

#df1.write.mode("append").option('path','/mnt/bronzelayer/agent').saveAsTable('bronzelayer.agent')

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Check if table is accessible and showing data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronzelayer.agent

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Move the loaded file from landing folder to processed folder using the proper date-time column mm-dd-yyyy folder

# COMMAND ----------

from datetime import datetime 
def createCurrentTimeFolder():
    currentDate = datetime.now()
    folderName = currentDate.strftime('%m-%d-%Y')
    return '/mnt/processed/Branch/' + folderName


destFolder =createCurrentTimeFolder()

dbutils.fs.mv('/mnt/landing/Branch',destFolder,True)

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Extension: You can add the test cases for all the steps

# COMMAND ----------

#Write your code here
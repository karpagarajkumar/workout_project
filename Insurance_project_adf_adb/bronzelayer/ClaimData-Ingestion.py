# Databricks notebook source
# MAGIC %md
# MAGIC <b> Load the Claim data from landing folder to Dataframe with extra column 'merge_flag'. </b>
# MAGIC <br> Use the schema as: claim_id int, policy_id int, date_of_claim timestamp, claim_amount double, claim_status string, LastUpdatedTimeStamp timestamp
# MAGIC <br>
# MAGIC <b> Save it as Claim Table with location as bronzelayer/Claim

# COMMAND ----------

from pyspark.sql.functions import lit

claim_schema='claim_id int, policy_id int, date_of_claim timestamp, claim_amount double, claim_status string, LastUpdatedTimeStamp timestamp'

df=spark.read.parquet('/mnt/landing/claim', schema=claim_schema)

df1=df.withColumn('merge_flag', lit(False))

df1.write.mode('append').option('path','/mnt/bronzelayer/claim').saveAsTable('bronzelayer.claim')

#display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronzelayer.claim

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Check if table is accessible and showing data

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Move the loaded file from landing folder to processed folder using the proper date-time column mm-dd-yyyy folder

# COMMAND ----------

#Write your code here

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Extension: You can add the test cases for all the steps

# COMMAND ----------

#Write your code here
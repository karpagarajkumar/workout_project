# Databricks notebook source
# MAGIC %md
# MAGIC <b> Load the Policy data from landing folder to Dataframe with extra column 'merge_flag'. </b>
# MAGIC <br> Use the schema as: policy_id int, policy_type string, customer_id int, start_date timestamp, end_date timestamp, premium double,  coverage_amount double
# MAGIC <br>
# MAGIC <b> Save it as Policy Table with location as bronzelayer/Policy

# COMMAND ----------

from pyspark.sql.functions import lit

policy_schema="policy_id int, policy_type string, customer_id int, start_date timestamp, end_date timestamp, premium double, coverage_amount double"

df=spark.read.json('/mnt/landing/policy', schema=policy_schema)

df1=df.withColumn('merge_flag', lit(False))

df1.write.mode("append").option('path','/mnt/bronzelayer/policy').saveAsTable('bronzelayer.policy')

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Check if table is accessible and showing data

# COMMAND ----------

# MAGIC %sql
# MAGIC select policy_type,sum(coverage_amount) from bronzelayer.policy group by policy_type order by policy_type

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
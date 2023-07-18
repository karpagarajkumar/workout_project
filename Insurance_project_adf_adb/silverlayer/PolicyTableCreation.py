# Databricks notebook source
# MAGIC %md
# MAGIC <b>
# MAGIC Create Table Policy in the silver layer database pointing to silver layer folder location in the ADLS. Use delta format

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table silverlayer.Policy(
# MAGIC
# MAGIC policy_id int, 
# MAGIC policy_type string, 
# MAGIC customer_id int, 
# MAGIC start_date timestamp, 
# MAGIC end_date timestamp, 
# MAGIC premium double,  
# MAGIC coverage_amount double,
# MAGIC merged_timestamp timestamp
# MAGIC
# MAGIC ) using delta location '/mnt/silverlayer/Policy'
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Quick Verify table is created or not.

# COMMAND ----------

#write your code here

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Extension: You can add the test cases for all the steps

# COMMAND ----------

#write your code here
# Databricks notebook source
# MAGIC %md
# MAGIC <b>
# MAGIC Create Table Branch in the silver layer database pointing to silver layer folder location in the ADLS. Use delta format

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table silverlayer.Branch(
# MAGIC branch_id int, branch_country string, branch_city string,merge_timestamp timestamp
# MAGIC )using delta location "/mnt/silverlayer/Branch"
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Quick Verify table is created or not.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.Branch

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Extension: You can add the test cases for all the steps

# COMMAND ----------

#write your code here
# Databricks notebook source
# MAGIC %md
# MAGIC <b>
# MAGIC Create Table Agent in the silver layer database pointing to silver layer folder location in the ADLS. Use delta format

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table silverlayer.Agent (
# MAGIC agent_id integer, 
# MAGIC agent_name string, 
# MAGIC agent_email string,
# MAGIC agent_phone string, 
# MAGIC branch_id integer, 
# MAGIC create_timestamp timestamp
# MAGIC
# MAGIC ) using delta location '/mnt/silverlayer/Agent'
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Quick Verify table is created or not.

# COMMAND ----------

#write your code here

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Extension: You can add the test cases for all the steps
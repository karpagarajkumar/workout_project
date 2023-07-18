# Databricks notebook source
# MAGIC %md
# MAGIC <b>
# MAGIC Create Table Claim in the silver layer database pointing to silver layer folder location in the ADLS. Use delta format

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table silverlayer.Claim(
# MAGIC claim_id int, 
# MAGIC policy_id int, 
# MAGIC date_of_claim timestamp,
# MAGIC claim_amount double,
# MAGIC claim_status string, 
# MAGIC LastUpdatedTimeStamp timestamp,
# MAGIC merged_timestamp timestamp
# MAGIC ) using delta location '/mnt/silverlayer/Claim'

# COMMAND ----------

#write your code here
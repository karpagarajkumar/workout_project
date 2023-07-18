# Databricks notebook source
# MAGIC %md
# MAGIC <b>
# MAGIC Create Table Customer in the silver layer database pointing to silver layer folder location in the ADLS. Use delta format

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table silverlayer.Customer (
# MAGIC customer_id int,
# MAGIC first_name string ,
# MAGIC last_name string, 
# MAGIC email string,
# MAGIC phone string,
# MAGIC country string, 
# MAGIC city string, 
# MAGIC registration_date timestamp, 
# MAGIC date_of_birth timestamp, 
# MAGIC gender string,
# MAGIC merged_timestamp timestamp
# MAGIC
# MAGIC ) using delta location '/mnt/silverlayer/Customer'
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
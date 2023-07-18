# Databricks notebook source
# MAGIC %md
# MAGIC <b> Create SilverLayer Database

# COMMAND ----------

spark.sql("create database silverlayer")

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Ensure this test case get pass

# COMMAND ----------

def test_create_database():
  databases = spark.sql("SHOW DATABASES LIKE 'silverlayer'")
  assert databases.count() == 0, "Database was not created"
  
test_create_database()
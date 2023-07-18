# Databricks notebook source
# MAGIC %md
# MAGIC <b> Create BronzeLayer Database

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Ensure this test case get pass

# COMMAND ----------

spark.sql('create database bronzelayer')

# COMMAND ----------

def test_create_database():
  databases = spark.sql("SHOW DATABASES LIKE 'bronzelayer'")
  assert databases.count() == 0, "Database was not created"
  
test_create_database()
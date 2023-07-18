# Databricks notebook source
# MAGIC %md
# MAGIC <b> Load the Branch data from landing folder to Dataframe with extra column 'merge_flag'. </b>
# MAGIC <br> Use the schema as: branch_id int, branch_country string, branch_city string
# MAGIC <br>
# MAGIC <b> Save it as Branch Table with location as bronzelayer/Branch

# COMMAND ----------

from pyspark.sql.functions import lit

branch_schema='branch_id int, branch_country string, branch_city string'

df=spark.read.parquet('/mnt/landing/Branch', schema=branch_schema)

df1=df.withColumn('merge_flag', lit(False))

df1.write.mode('append').option('path','/mnt/bronzelayer/Branch').saveAsTable('bronzelayer.branch')

#display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Check if table is accessible and showing data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronzelayer.branch
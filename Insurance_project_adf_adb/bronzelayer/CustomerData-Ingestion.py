# Databricks notebook source
# MAGIC %md
# MAGIC <b> Load the Customer data from landing folder to Dataframe with extra column 'merge_flag'. </b>
# MAGIC <br> Use the schema as: customer_id int,first_name string ,last_name string, email string, phone string, country string, city string, registration_date timestamp, date_of_birth timestamp, gender string
# MAGIC <br>
# MAGIC <b> Save it as Customer Table with location as bronzelayer/Customer

# COMMAND ----------

from pyspark.sql.functions import lit

customer_schema="customer_id int,first_name string ,last_name string, email string, phone string, country string, city string, registration_date timestamp, date_of_birth timestamp, gender string"

df=spark.read.csv('/mnt/landing/customer', schema=customer_schema)

df1=df.withColumn('merge_flag', lit(False))

df1.write.mode("append").option('path','/mnt/bronzelayer/customer').saveAsTable('bronzelayer.customer')

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Check if table is accessible and showing data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronzelayer.customer

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
# Databricks notebook source
# MAGIC %md
# MAGIC <b>Remove all where Customer Id not null

# COMMAND ----------

#df = spark.sql("select * from bronzelayer.customer where customer_id is not null and registration_date > date_of_birth") 
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b> 
# MAGIC Remove records where gender is other than Male/Female

# COMMAND ----------

# MAGIC %sql
# MAGIC --df = spark.sql("select * from bronzelayer.customer where customer_id is not null and gender in ('male','female')")
# MAGIC --display(df)
# MAGIC
# MAGIC show create table silverlayer.customer

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Outlier check at some registration_date > DOb.

# COMMAND ----------

df = spark.sql("select * from bronzelayer.customer where customer_id is not null and gender in ('Male','Female') and registration_date > date_of_birth")
display(df)
df.createOrReplaceTempView("clean_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Merge the data into silver layer while adding current_timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE into silverlayer.customer as s using clean_customer as b on s.customer_id=b.customer_id
# MAGIC when matched then update set s.first_name=b.first_name, s.last_name=b.last_name, s.email=b.email, s.phone=b.phone, s.country=b.country, s.city=b.city, s.registration_date=b.registration_date, s.date_of_birth=b.date_of_birth, s.gender=b.gender, s.merged_timestamp = current_timestamp()
# MAGIC
# MAGIC when not matched then insert (customer_id, first_name, last_name, email, phone, country, city, registration_date, date_of_birth, gender,  merged_timestamp) values (b.customer_id, b.first_name, b.last_name, b.email, b.phone, b.country, b.city, b.registration_date, b.date_of_birth, b.gender,  current_timestamp())
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Update the flag in the bronze layer

# COMMAND ----------

# MAGIC %sql
# MAGIC update bronzelayer.customer set merge_flag = True where merge_flag = False
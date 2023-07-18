# Databricks notebook source
# MAGIC %md
# MAGIC <b> Load all the new rows from bronze layer Agent table which is yet not merged

# COMMAND ----------

df= spark.sql("select * from bronzelayer.agent where merge_flag=False")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Remove all rows where Barnch Id not exist in Branch table

# COMMAND ----------

df= spark.sql("select a.* from bronzelayer.agent a join bronzelayer.branch b on a.branch_id=b.branch_id  where a.merge_flag=False and len(a.agent_phone)=10")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Ensure all the phone have valid 10 digit phone no.

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC a.agent_id,
# MAGIC a.agent_name,
# MAGIC case 
# MAGIC when a.agent_email =''
# MAGIC then 'abcd@gmail.com'
# MAGIC else a.agent_email end as agent_email,
# MAGIC a.agent_phone,
# MAGIC a.branch_id,
# MAGIC create_timestamp,
# MAGIC current_timestamp() as merged_timestamp
# MAGIC from bronzelayer.agent a join bronzelayer.branch b 
# MAGIC on a.branch_id=b.branch_id  
# MAGIC where a.merge_flag=False and len(a.agent_phone)=10
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import expr
df= spark.sql("select a.* from bronzelayer.agent a join bronzelayer.branch b on a.branch_id=b.branch_id  where a.merge_flag=False and len(a.agent_phone)=10")
df1=df.withColumn("agent_email",
                  expr("case when agent_email= '' then 'abcd@gmail.com' else agent_email end"))
df1=df1.select(df1.agent_id,df1.agent_name,df1.agent_email,df1.agent_phone,df1.branch_id,df1.create_timestamp).display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Replace all the empty email with 'admin@azurelib.com'

# COMMAND ----------

from pyspark.sql.functions import *
bronze_agent_df= spark.sql("select * from bronzelayer.agent")
bronze_branch_df= spark.sql("select * from bronzelayer.branch")
silver_agent_df=spark.sql("select * from silverlayer.agent")
join_df = bronze_agent_df.join(bronze_branch_df, ["branch_id"] ,"inner").select(bronze_agent_df["*"])
join_df1= join_df.where( (join_df.merge_flag  == False) & (length(join_df.agent_phone)==10))
join_df1=join_df1.withColumn("agent_email",
                  expr("case when agent_email= '' then 'abcd@gmail.com' else agent_email end"))
join_df1=join_df1.select(join_df1.agent_id,join_df1.agent_name,join_df1.agent_email,join_df1.agent_phone,join_df1.branch_id,join_df1.create_timestamp)
join_df1.createOrReplaceTempView("clean_agent")

#df.createOrReplaceTempView("clean_branch")

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Add the merged_date_timestamp (current timesatmp)

# COMMAND ----------

#df3=spark.sql("select * from silverlayer.agent").withColumn("merged_timestamp", lit(None).cast('timestamp'))
#df3.write.mode("overwrite").option("mergeSchema", "true").option("path",'/mnt/silverlayer/Agent').saveAsTable("silverlayer.agent")
df4=spark.sql("select * from silverlayer.agent").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE into silverlayer.agent as s using clean_agent as b on s.agent_id=b.agent_id
# MAGIC when matched then update set s.agent_name=b.agent_name, s.agent_email=b.agent_email, s.agent_phone=b.agent_phone, s.branch_id=b.branch_id, s.create_timestamp=b.create_timestamp, s.merged_timestamp = current_timestamp()
# MAGIC
# MAGIC when not matched then insert (agent_id, agent_name, agent_email, agent_phone, branch_id, create_timestamp, merged_timestamp) values (b.agent_id, b.agent_name, b.agent_email, b.agent_phone, b.branch_id, b.create_timestamp, current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC update bronzelayer.agent set merge_flag = True where merge_flag = False
# Databricks notebook source
# MAGIC %md
# MAGIC <b>Remove all where claim_id, policy_id is ,claim status,claim_amount,lastupdated null

# COMMAND ----------

df5=spark.sql("select * from bronzelayer.claim where claim_id is not null or policy_id is not null or claim_status is not null or claim_amount is not null or LastUpdatedTimeStamp is not null").display(df5)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Remove all rows where policy_id Id not exist in Policy table

# COMMAND ----------

df5=spark.sql("select c.* from bronzelayer.claim c inner join bronzelayer.policy p on c.policy_id = p.policy_id where c.claim_id is not null or c.policy_id is not null or c.claim_status is not null or c.claim_amount is not null or c.LastUpdatedTimeStamp is not null").display(df5)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Convert date_of_claim to Date column with formate (MM-dd-yyyy)

# COMMAND ----------

#from pyspark.sql.functions import *
df5=spark.sql("select c.claim_id,c.policy_id, to_date(cast(c.date_of_claim as date),'MM-dd-yyyy') as date_of_claim, c.claim_amount, c.claim_status, c.LastUpdatedTimeStamp from bronzelayer.claim c inner join bronzelayer.policy p on c.policy_id = p.policy_id where c.claim_id is not null or c.policy_id is not null or c.claim_status is not null or c.claim_amount is not null or c.LastUpdatedTimeStamp is not null").display(df5)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Ensure  claim amount is >0

# COMMAND ----------

df5=spark.sql("select c.claim_id,c.policy_id, to_date(cast(c.date_of_claim as date),'MM-dd-yyyy') as date_of_claim, c.claim_amount, c.claim_status, c.LastUpdatedTimeStamp from bronzelayer.claim c inner join bronzelayer.policy p on c.policy_id = p.policy_id where c.claim_amount>0 and (c.claim_id is not null or c.policy_id is not null or c.claim_status is not null or c.claim_amount is not null or c.LastUpdatedTimeStamp is not null)")
df5.createOrReplaceTempView("clean_claim")

# COMMAND ----------

spark.sql("select * from silverlayer.claim").show()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Add the merged_date_timestamp (current timesatmp)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE into silverlayer.claim as s using clean_claim as b on s.claim_id=b.claim_id
# MAGIC when matched then update set s.policy_id=b.policy_id, s.date_of_claim=b.date_of_claim, s.claim_amount=b.claim_amount, s.claim_status=b.claim_status, s.LastUpdatedTimeStamp=b.LastUpdatedTimeStamp, s.merged_timestamp = current_timestamp()
# MAGIC
# MAGIC when not matched then insert (claim_id, policy_id, date_of_claim, claim_amount, claim_status, LastUpdatedTimeStamp, merged_timestamp) values (b.claim_id, b.policy_id, b.date_of_claim, b.claim_amount, b.claim_status, b.LastUpdatedTimeStamp, current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Update Flag

# COMMAND ----------

# MAGIC %sql
# MAGIC update bronzelayer.claim set merge_flag = True where merge_flag = False

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.claim
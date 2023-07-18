# Databricks notebook source
# MAGIC %md
# MAGIC <b>Remove all the rows where Customer Id,Policy ID is null

# COMMAND ----------

df6=spark.sql("select * from bronzelayer.policy where customer_id is not null or policy_id is not null").display(df6)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Remove all rows where Customer Id not exist in Customer table

# COMMAND ----------

df6=spark.sql("select p.policy_id, p.policy_type, p.customer_id, p.start_date, p.end_date, p.premium, p.coverage_amount from bronzelayer.policy p join bronzelayer.customer c on p.customer_id = c.customer_id where p.customer_id is not null or p.policy_id is not null limit 1").display(df6)


# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Every policy must have preminum & covergae amount >0

# COMMAND ----------

df6=spark.sql("select p.policy_id, p.policy_type, p.customer_id, p.start_date, p.end_date, p.premium, p.coverage_amount from bronzelayer.policy p join bronzelayer.customer c on p.customer_id = c.customer_id where p.premium > 0 and p.coverage_amount > 0 and (p.customer_id is not null or p.policy_id is not null)").display(df6)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Validate end_date>start_date

# COMMAND ----------

df6=spark.sql("select p.policy_id, p.policy_type, p.customer_id, p.start_date, p.end_date, p.premium, p.coverage_amount from bronzelayer.policy p join bronzelayer.customer c on p.customer_id = c.customer_id where p.end_date > p.start_date and p.premium > 0 and p.coverage_amount > 0 and (p.customer_id is not null or p.policy_id is not null)")
df6.createOrReplaceTempView("clean_policy")

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Merged the table with merged_date_timestamp as current timesatmp

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE into silverlayer.policy as s using clean_policy as b on s.policy_id=b.policy_id
# MAGIC when matched then update set s.policy_type=b.policy_type, s.customer_id=b.customer_id, s.start_date=b.start_date, s.end_date=b.end_date, s.premium=b.premium, s.coverage_amount=b.coverage_amount, s.merged_timestamp = current_timestamp()
# MAGIC
# MAGIC when not matched then insert (policy_id, policy_type, customer_id, start_date, end_date, premium, coverage_amount, merged_timestamp) values (b.policy_id, b.policy_type, b.customer_id, b.start_date, b.end_date, b.premium, coverage_amount, current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Update the merged_flag in the bronze layer

# COMMAND ----------

# MAGIC %sql
# MAGIC update bronzelayer.policy set merge_flag = True where merge_flag = False

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Extension: You can add the test cases for all the steps

# COMMAND ----------

#Write your code here

# COMMAND ----------

spark.sql(" select * from bronzelayer.policy").take(2) 
# Databricks notebook source
# MAGIC %md
# MAGIC <b>Remove all where brnach_id not null

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC <b>Remove all the leading and trailing spaces in Brnach Country and covert it into UPPER CASE

# COMMAND ----------

df = spark.sql("select branch_id, upper(trim(branch_country)) as branch_country, branch_city from bronzelayer.branch where merge_flag = False and branch_id is not null")

df.createOrReplaceTempView("clean_branch")

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Merge into Silver layer table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO silverlayer.branch AS T USING clean_branch AS S ON T.branch_id = S.branch_id
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET T.branch_country = S.branch_country, T.branch_city = S.branch_city, T.merge_timestamp = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (branch_id, branch_country, branch_city, merge_timestamp) VALUES (S.branch_id, S.branch_country, S.branch_city, current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Update the merged_flag in the bronzelayer table

# COMMAND ----------

# MAGIC %sql
# MAGIC update bronzelayer.branch set merge_flag = True where merge_flag = False
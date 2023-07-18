# Databricks notebook source
#create dataframe for metadata in databricks
spark.sql("create database goldlayer")

# COMMAND ----------

#sales by month and policy

df=spark.sql("select policy_type,extract( Month from start_date) as Month, cast(sum(premium) as integer) as premium from silverlayer.policy group by 1,2 order by 2")
df.write.option('path','/mnt/goldlayer/rpt_policy_month').saveAsTable('goldlayer.rpt_policy_month')
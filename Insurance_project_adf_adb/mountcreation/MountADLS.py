# Databricks notebook source
# MAGIC %md
# MAGIC <b> Mount the Bronze Layer Container:
# MAGIC dbutils.fs.mount( source = 'wasbs://bronzelayer@policysystemadls.blob.core.windows.net',
# MAGIC                  mount_point= '/mnt/bronzelayer', extra_configs ={'fs.azure.sas.bronzelayer.policysystemadls.blob.core.windows.net':'?sv=2021-12-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-05-05T22:11:54Z&st=2023-04-17T14:11:54Z&spr=https&sig=2J%2BKZHI5Bc%2Bzin1m8H7wEXtV%2FRWdhVx8VYqve%2Ft9070%3D'})
# MAGIC

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://bronzelayer@sampleprojectadls.blob.core.windows.net',
                 mount_point= '/mnt/bronzelayer', 
                 extra_configs ={'fs.azure.sas.bronzelayer.sampleprojectadls.blob.core.windows.net':'?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-10-07T05:06:28Z&st=2023-06-27T21:06:28Z&spr=https&sig=C6OBTLkKmyATF3yleGeJzoEpXGwLVPWnkEU06w6y%2FF0%3D'})


# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://processed@sampleprojectadls.blob.core.windows.net',
                 mount_point= '/mnt/processed', 
                 extra_configs ={'fs.azure.sas.processed.sampleprojectadls.blob.core.windows.net':'?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-10-07T05:06:28Z&st=2023-06-27T21:06:28Z&spr=https&sig=C6OBTLkKmyATF3yleGeJzoEpXGwLVPWnkEU06w6y%2FF0%3D'})

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://silverlayer@sampleprojectadls.blob.core.windows.net',
                 mount_point= '/mnt/silverlayer', 
                 extra_configs ={'fs.azure.sas.silverlayer.sampleprojectadls.blob.core.windows.net':'?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-10-07T05:06:28Z&st=2023-06-27T21:06:28Z&spr=https&sig=C6OBTLkKmyATF3yleGeJzoEpXGwLVPWnkEU06w6y%2FF0%3D'})

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://goldlayer@sampleprojectadls.blob.core.windows.net',
                 mount_point= '/mnt/goldlayer', 
                 extra_configs ={'fs.azure.sas.goldlayer.sampleprojectadls.blob.core.windows.net':'?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-10-07T05:06:28Z&st=2023-06-27T21:06:28Z&spr=https&sig=C6OBTLkKmyATF3yleGeJzoEpXGwLVPWnkEU06w6y%2FF0%3D'})

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Checking Bronze Layer Mount point

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Mount the Landing Container:

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://landing@sampleprojectadls.blob.core.windows.net',
                 mount_point= '/mnt/landing', 
                 extra_configs ={'fs.azure.sas.landing.sampleprojectadls.blob.core.windows.net':'?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-10-07T05:06:28Z&st=2023-06-27T21:06:28Z&spr=https&sig=C6OBTLkKmyATF3yleGeJzoEpXGwLVPWnkEU06w6y%2FF0%3D'})

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://crimeprojcontainer@sampleprojectadls.blob.core.windows.net',
                 mount_point= '/mnt/crimedata', 
                 extra_configs ={'fs.azure.sas.crimeprojcontainer.sampleprojectadls.blob.core.windows.net':'?sp=r&st=2023-07-08T11:16:35Z&se=2023-07-27T19:16:35Z&spr=https&sv=2022-11-02&sr=c&sig=HiC%2Bj0XTy77aiaA8cZ2YRzn3qdFGpygag0c23dsVqFw%3D'})

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://bronzecrime@sampleprojectadls.blob.core.windows.net',
                 mount_point= '/mnt/bronzecrime', 
                 extra_configs ={'fs.azure.sas.bronzecrime.sampleprojectadls.blob.core.windows.net':'?sp=r&st=2023-07-08T12:03:10Z&se=2023-07-26T20:03:10Z&spr=https&sv=2022-11-02&sr=c&sig=n8vTbngmDSZ0sC70qAMDotQvvnVhHCaCkTMcmpOZbk4%3D'})

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://crimesilver@sampleprojectadls.blob.core.windows.net',
                 mount_point= '/mnt/crimesilver', 
                 extra_configs ={'fs.azure.sas.crimesilver.sampleprojectadls.blob.core.windows.net':'sp=r&st=2023-07-08T13:56:41Z&se=2023-07-27T21:56:41Z&spr=https&sv=2022-11-02&sr=c&sig=wEiR%2Fznmp%2FxEU6nAou3YukRmT7%2BDsuFwkA6vJqQsjSs%3D'})

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Checking landing Mount point

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/
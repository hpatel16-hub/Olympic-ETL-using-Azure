# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='Tokyo_Olympics')

# COMMAND ----------

ClientID = dbutils.secrets.get(scope='Tokyo_Olympics', key='ClientID')
TenantID = dbutils.secrets.get(scope='Tokyo_Olympics', key='TenantID')
SecretKey = dbutils.secrets.get(scope='Tokyo_Olympics', key='SecretKey')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": ClientID,
"fs.azure.account.oauth2.client.secret": SecretKey,
"fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{TenantID}/oauth2/token"}




# COMMAND ----------


# Mount the directory
dbutils.fs.mount(
source = "abfss://tokyo-olympics@harshilvehicleproject.dfs.core.windows.net", # container@storageacc
mount_point = "/mnt/tokyoolympic",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolympic"

# COMMAND ----------

spark

# COMMAND ----------

Athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/Raw-Data/Athletes.csv")
Coaches = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/Raw-Data/Coaches.csv")
EntriesGender = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/Raw-Data/EntriesGender.csv")
Medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolypmic/Raw-Data/Medals.csv")
Teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolypmic/Raw-Data/Teams.csv")
     

# COMMAND ----------

Athletes.show()

# COMMAND ----------

# Find the top countries with the highest number of gold medals
top_gold_medal_countries = Medals.orderBy("Gold", ascending=False).select("Team_Country","Gold").show()
     

# COMMAND ----------

# Calculate the average number of entries by gender for each discipline
average_entries_by_gender = EntriesGender.withColumn(
    'Avg_Female', EntriesGender['Female'] / EntriesGender['Total']
).withColumn(
    'Avg_Male', EntriesGender['Male'] / EntriesGender['Total']
)
average_entries_by_gender.show()

# COMMAND ----------

Athletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolympic/Transform-Data/Athletes")
Coaches.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolympic/Transform-Data/Coaches")
EntriesGender.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolympic/Transform-Data/EntriesGender")
Medals.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolympic/Transform-Data/Medals")
Teams.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolympic/Transform-Data/Teams")
     

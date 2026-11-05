# Databricks notebook source
# MAGIC %run ../../Includes/Classroom-Setup-05.2.4L

# COMMAND ----------

# DBTITLE 1,Markdown
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Exploring the Results of an SDP pipeline
# MAGIC
# MAGIC Run the following cell to enumerate the output of your storage location:

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.storage_location)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC The **system** directory captures events associated with the pipeline.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/system/events")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC These event logs are stored as a Delta table. 
# MAGIC
# MAGIC Let's query the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${DA.paths.storage_location}/system/events`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Let's view the contents of the *tables* directory.

# COMMAND ----------

# On current SDP runtimes, tables published to a target schema are no longer
# written under <storage_location>/tables; fall back to listing the schema's tables.
try:
    files = dbutils.fs.ls(f"{DA.paths.storage_location}/tables")
    display(files)
except Exception:
    display(spark.sql(f"SHOW TABLES IN {DA.schema_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Let's query the gold table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${DA.schema_name}.daily_patient_avg

# COMMAND ----------

DA.cleanup()

# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

# DBTITLE 1,Lesson setup
DA = DBAcademyHelper(course_config, lesson_config)
DA.reset_lesson()
DA.init()

# DBR 18 / Standard access mode: sqlContext and spark.sparkContext are not available.
try:
    spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
except Exception:
    spark.conf.set("spark.sql.shuffle.partitions", 8)

DA.conclude_setup()

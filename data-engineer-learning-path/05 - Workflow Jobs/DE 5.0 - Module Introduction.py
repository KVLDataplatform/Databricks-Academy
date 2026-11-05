# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Markdown
# MAGIC %md
# MAGIC ## Orchestration with Lakeflow Jobs
# MAGIC This module is part of the Data Engineer Learning Path by Databricks Academy.
# MAGIC
# MAGIC #### Lessons
# MAGIC Lecture: Introduction to Lakeflow Jobs <br>
# MAGIC Lecture: Building and Monitoring Lakeflow Jobs <br>
# MAGIC Demo: Building and Monitoring Lakeflow Jobs <br>
# MAGIC DE 5.1 - Scheduling Tasks with the Jobs UI <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.1.1 - Task Orchestration]($./DE 5.1 - Scheduling Tasks with the Jobs UI/DE 5.1.1 - Task Orchestration) <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.1.2 - Reset]($./DE 5.1 - Scheduling Tasks with the Jobs UI/DE 5.1.2 - Reset) <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.1.3 - SDP Job]($./DE 5.1 - Scheduling Tasks with the Jobs UI/DE 5.1.3 - DLT Job) <br>
# MAGIC DE 5.2L - Jobs Lab <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.2.1L - Lab Instructions]($./DE 5.2L - Jobs Lab/DE 5.2.1L - Lab Instructions) <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.2.2L - Batch Job]($./DE 5.2L - Jobs Lab/DE 5.2.2L - Batch Job) <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.2.3L - SDP Job]($./DE 5.2L - Jobs Lab/DE 5.2.3L - DLT Job) <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.2.4L - Query Results Job]($./DE 5.2L - Jobs Lab/DE 5.2.4L - Query Results Job) <br>
# MAGIC
# MAGIC #### Prerequisites
# MAGIC * Ability to perform basic code development tasks using the Databricks workspace (create clusters, run code in notebooks, use basic notebook operations, import repos from git, etc)
# MAGIC * Ability to configure and run data pipelines using the Lakeflow Spark Declarative Pipelines UI
# MAGIC * Beginner experience defining Lakeflow Spark Declarative Pipelines (SDP) using PySpark
# MAGIC   * Ingest and process data using Auto Loader and PySpark syntax
# MAGIC   * Process Change Data Capture feeds with APPLY CHANGES INTO syntax
# MAGIC * Review pipeline event logs and results to troubleshoot SDP syntax
# MAGIC * Production experience working with data warehouses and data lakes
# MAGIC
# MAGIC
# MAGIC #### Technical Considerations
# MAGIC * This course runs on DBR 18.x (tested on 18.x ML, Scala 2.13).
# MAGIC * This course cannot be delivered on Databricks Community Edition.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
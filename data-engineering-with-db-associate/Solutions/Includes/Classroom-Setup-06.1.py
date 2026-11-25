# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

class DataFactory:
    def __init__(self, ):
        self.source = f"{DA.paths.datasets}/healthcare/tracker/streaming"
        self.userdir = f"{DA.paths.working_dir}/tracker"
        self.curr_mo = 1
    
    def load(self, continuous=False):
        if self.curr_mo > 12:
            print("Data source exhausted\n")
            
        elif continuous == True:
            while self.curr_mo <= 12:
                curr_file = f"{self.curr_mo:02}.json"
                dbutils.fs.cp(self.source + curr_file, self.userdir + curr_file)
                self.curr_mo += 1
        else:
            curr_file = f"{str(self.curr_mo).zfill(2)}.json"
            target_dir = f"{self.userdir}/{curr_file}"
            print(f"Loading the file {curr_file} to the tracker dataset")
            dbutils.fs.cp(f"{self.source}/{curr_file}", target_dir)
            self.curr_mo += 1

# COMMAND ----------

# DBTITLE 1,Lesson setup
DA = DBAcademyHelper(course_config, lesson_config)
DA.reset_lesson()
DA.init()

DA.data_factory = DataFactory()
print()

DA.data_factory.load()

# DBR 18 / Standard access mode: sqlContext and spark.sparkContext are not available.
try:
    spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
except Exception:
    spark.conf.set("spark.sql.shuffle.partitions", 8)

DA.conclude_setup()

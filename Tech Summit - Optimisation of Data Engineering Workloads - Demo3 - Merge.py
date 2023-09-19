# Databricks notebook source
# MAGIC %md
# MAGIC # Tech Summit &raquo; Optimisation of Data Engineering Workloads &raquo; Demo3 &raquo;&raquo; Merge

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generation

# COMMAND ----------

# DBTITLE 1,Install dbldatagen
# MAGIC %pip install dbldatagen

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r "/tmp/tech-summit/de-opt/merge/"

# COMMAND ----------

# DBTITLE 1,Imports
import dbldatagen as dg
from pyspark.sql.types import IntegerType, FloatType, StringType, DateType
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate 1B rows sales dataset with 100K unique customers

# COMMAND ----------

# DBTITLE 0,Generate 10M rows sales dataset with 10K unique customers
data_rows = 1000000000
df_spec = (dg.DataGenerator(spark, name="sales_data", rows=data_rows, partitions=100)
           .withColumn("customer_id", IntegerType(), minValue=1, maxValue=100000)
           .withColumn("transaction_id", IntegerType(), minValue=1, maxValue=2000000)
           .withColumn("order_id", IntegerType(), minValue=1, maxValue=2000000)
           .withColumn("amount", FloatType(), minValue=1.0, maxValue=500000.0)
           )

sales_df = df_spec.build() 
display(sales_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Write Landing sales dataframe to delta
(sales_df
  .write
  .mode("overwrite")
  .save("/tmp/tech-summit/de-opt/merge/target_sales_table")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate customer batch dataset with 1k rows, 1 row per customer

# COMMAND ----------

data_rows = 1000
df_spec = (dg.DataGenerator(spark, name="cust_data", rows=data_rows, partitions=4)
           .withColumn("customer_id", IntegerType(), minValue=1, maxValue=1000)
           .withColumn("transaction_id", IntegerType(), minValue=1, maxValue=2000000)
           .withColumn("order_id", IntegerType(), minValue=1, maxValue=2000000)
           .withColumn("amount", FloatType(), minValue=1.0, maxValue=500000.0)
           )

batch_df = df_spec.build().dropDuplicates(["customer_id"])
display(batch_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Baseline Merge

# COMMAND ----------

# MAGIC %md
# MAGIC ### Disable a few settings

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Disable AQE optimisations
# MAGIC set spark.sql.adaptive.skewJoin.enabled = false;
# MAGIC set spark.sql.adaptive.coalescePartitions.enabled = false;
# MAGIC -- Disable spark & AQE broadcasts
# MAGIC set spark.sql.autoBroadcastJoinThreshold = -1;
# MAGIC set spark.databricks.adaptive.autoBroadcastJoinThreshold = -1;
# MAGIC -- Disable LSM -- please note by default this is True. This is only for the demo.
# MAGIC set spark.databricks.delta.merge.enableLowShuffle = false;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Baseline Merge Performance

# COMMAND ----------

from delta.tables import *

target_table = DeltaTable.forPath(spark, "/tmp/tech-summit/de-opt/merge/target_sales_table")

(target_table.alias("target")
  .merge(batch_df.alias("source"), "target.customer_id = source.customer_id")
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
).execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimise and Z-Order the Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY delta.`/tmp/tech-summit/de-opt/merge/target_sales_table`

# COMMAND ----------

(
  spark
    .read
    .format("delta")
    .option("versionAsOf", 0)
    .load("/tmp/tech-summit/de-opt/merge/target_sales_table")
    .write
    .mode("overwrite")
    .save("/tmp/tech-summit/de-opt/merge/target_sales_table_zordered")
)

# COMMAND ----------

# DBTITLE 1,Set the Target file size to 64MB
spark.sql("ALTER TABLE delta.`/tmp/tech-summit/de-opt/merge/target_sales_table_zordered` SET TBLPROPERTIES ('delta.targetFileSize'=67108864)")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED delta.`/tmp/tech-summit/de-opt/merge/target_sales_table_zordered`

# COMMAND ----------

# DBTITLE 0,Optimize and Z-Order the table
deltaTable = DeltaTable.forPath(spark, "/tmp/tech-summit/de-opt/merge/target_sales_table_zordered")
display(deltaTable.optimize().executeZOrderBy("customer_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimised Merge

# COMMAND ----------

# DBTITLE 1,Find all customer_ids from the incremental source
customer_id_list = batch_df.select("customer_id").collect()
customer_ids = "'" + "','".join([str(row.customer_id) for row in customer_id_list]) + "'"
# customer_ids

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable various optimisations

# COMMAND ----------

# DBTITLE 0,Enable various optimisations
# MAGIC %sql
# MAGIC -- Disable AQE optimisations
# MAGIC set spark.sql.adaptive.skewJoin.enabled = false;
# MAGIC set spark.sql.adaptive.coalescePartitions.enabled = false;
# MAGIC -- Disable spark & AQE broadcasts
# MAGIC set spark.sql.autoBroadcastJoinThreshold = 209715200;
# MAGIC set spark.databricks.adaptive.autoBroadcastJoinThreshold = 209715200;
# MAGIC -- Enable LSM -- please note by default this is True. This is for demo purpose only.
# MAGIC set spark.databricks.delta.merge.enableLowShuffle = true;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimised Merge Performance

# COMMAND ----------

# DBTITLE 0,Optimised Merge Performance
from delta.tables import *

target_table = DeltaTable.forPath(spark, "/tmp/tech-summit/de-opt/merge/target_sales_table_zordered")

(target_table.alias("target")
  .merge(broadcast(batch_df.alias("source")), 
         "target.customer_id IN (" + customer_ids + ") AND target.customer_id = source.customer_id"
        )
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
).execute()

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC # Optimisations applied
# MAGIC * Pipeline metrics:
# MAGIC   * Before: 156 seconds
# MAGIC   * After: 40 seconds
# MAGIC * Optimized the Delta table and Z-ordered on `customer_id`
# MAGIC   * Specified smaller file size explicitly
# MAGIC * Enabled Spark & AQE broadcast threshold to 200MB
# MAGIC   * Enabled Low-shuffle Merge
# MAGIC * Added additional condition in the merge statement to include the `customer_id`s from the source

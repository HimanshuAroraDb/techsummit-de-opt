# Databricks notebook source
# MAGIC %md
# MAGIC # Tech Summit - Optimisation of Data Engineering Workloads - Demo2 - Data Spill

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generation

# COMMAND ----------

# DBTITLE 1,Install dbldatagen
# MAGIC %pip install dbldatagen

# COMMAND ----------

# DBTITLE 1,Imports
import dbldatagen as dg
from pyspark.sql.types import IntegerType, FloatType, StringType, DateType
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Generate 10B rows sales dataset with 10K unique customers
data_rows = 10000000000
df_spec = (dg.DataGenerator(spark, name="sales_data", rows=data_rows,
                                                  partitions=64)
           .withColumn("customer_id", IntegerType(), minValue=1, maxValue=10000)
           .withColumn("transaction_id", IntegerType(), minValue=10000001, maxValue=20000000)
           .withColumn("order_id", IntegerType(), minValue=20000001, maxValue=30000000)
           .withColumn("amount", IntegerType(), minValue=30000001, maxValue=40000000)
           )   
sales_df = df_spec.build() 

# COMMAND ----------

display(sales_df)

# COMMAND ----------

# DBTITLE 1,Write sales dataframe to delta
sales_df.write.mode("overwrite").save("/tmp/tech-summit/de-opt/spill/sales_table")

# COMMAND ----------

# DBTITLE 1,Read sales delta table
sales_df = spark.read.format("delta").load("/tmp/tech-summit/de-opt/spill/sales_table")

# COMMAND ----------

# DBTITLE 1,Generate customer dimension dataset
data_rows = 10000 
df_spec = (dg.DataGenerator(spark, name="cust_data", rows=data_rows,
                                                  partitions=4)
           .withColumn("customer_id", IntegerType(), minValue=1, maxValue=10000)
           .withColumn("c", IntegerType(), 
                            minValue=40000000, maxValue=50000000,
                            numColumns=2)
           )
                            
cust_df = df_spec.build()

# COMMAND ----------

display(cust_df)

# COMMAND ----------

# DBTITLE 1,Write customer dataframe to delta
cust_df.write.mode("overwrite").save("/tmp/tech-summit/de-opt/spill/cust_table")

# COMMAND ----------

# DBTITLE 1,Read customer delta table
cust_df = spark.read.format("delta").load("/tmp/tech-summit/de-opt/spill/cust_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Without Any Optimisation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Disable AQE optimisations
# MAGIC set spark.sql.adaptive.skewJoin.enabled = false;
# MAGIC set spark.sql.adaptive.coalescePartitions.enabled = false;
# MAGIC -- Disable spark & AQE broadcasts
# MAGIC set spark.sql.autoBroadcastJoinThreshold = -1;
# MAGIC set spark.databricks.adaptive.autoBroadcastJoinThreshold = -1;

# COMMAND ----------

# DBTITLE 1,Join sales and customer dataframes
join_df = sales_df.join(cust_df, "customer_id", "left")

# COMMAND ----------

# DBTITLE 1,Simulate write op with noop
join_df.write.format("noop").mode("overwrite").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join with AQE AutoOptimizeShuffle (AOS) Optimisation

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.sql.shuffle.partitions=auto

# COMMAND ----------

# DBTITLE 1,Simulate write op with noop
join_df.write.format("noop").mode("overwrite").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manually Fine Tune Number Of Shuffle Partitions

# COMMAND ----------

# MAGIC %md
# MAGIC ![Shuffle Size](https://github.com/HimanshuAroraDb/Images/blob/master/shuffle-size.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - Total number of total worker cores in cluster (T) = 128
# MAGIC - Total amount of data being shuffled in shuffle stage in megabytes (B) = 167000
# MAGIC - Let's say the apporx optimal size of data to be processed per task in megabytes = 128
# MAGIC
# MAGIC - Hence the multiplication factor (M) = ceiling(B / 128 / T) = ceiling(167000 / 128 / 128) = 11
# MAGIC - **And the number of shuffle partitions (N) = M x T = 11 x 128 = 1408**

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.sql.shuffle.partitions=1408;

# COMMAND ----------

# DBTITLE 1,Simulate write op with noop
join_df.write.format("noop").mode("overwrite").save()

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC # Optimisations applied
# MAGIC * Pipeline metrics:
# MAGIC   * Before: 252 seconds
# MAGIC   * After:  186 seconds
# MAGIC * AQE auto shuffle partition tuning optimization
# MAGIC * Manual shuffle partition fine tuning

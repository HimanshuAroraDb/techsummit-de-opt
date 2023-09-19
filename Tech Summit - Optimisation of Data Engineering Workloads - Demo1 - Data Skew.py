# Databricks notebook source
# MAGIC %md
# MAGIC # Tech Summit - Optimisation of Data Engineering Workloads - Demo1 - Data Skew

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

# DBTITLE 1,Generate 10M rows sales dataset with 2K unique customers
data_rows = 10000000
df_spec = (dg.DataGenerator(spark, name="sales_data", rows=data_rows,
                                                  partitions=4)
           .withColumn("transaction_id", IntegerType(), minValue=1, maxValue=2000000)
           .withColumn("customer_id", IntegerType(), minValue=1, maxValue=2000)
           .withColumn("order_id", IntegerType(), minValue=1, maxValue=2000000)
           .withColumn("amount", FloatType(), minValue=1.0, maxValue=500000.0)
           )
                            
sales_df = df_spec.build() 

# COMMAND ----------

display(sales_df)

# COMMAND ----------

# DBTITLE 1,Each customer has 2K records in sales dataset
display(sales_df.groupBy("customer_id").count().orderBy(col("customer_id")))

# COMMAND ----------

# DBTITLE 1,Generate 100M rows sales dataset with customer_id = 1
data_rows = 100000000
df_spec = (
    dg.DataGenerator(spark, name="sales_data2", rows=data_rows, partitions=400)
    .withColumn("transaction_id", IntegerType(), minValue=1, maxValue=2000000)
    .withColumn("customer_id", IntegerType(), minValue=1, maxValue=1)
    .withColumn("order_id", IntegerType(), minValue=1, maxValue=2000000)
    .withColumn("amount", FloatType(), minValue=1.0, maxValue=500000.0)
)

sales_df2 = df_spec.build()

# COMMAND ----------

display(sales_df2)

# COMMAND ----------

display(sales_df2.groupBy("customer_id").count().orderBy(col("customer_id")))

# COMMAND ----------

# DBTITLE 1,Create skewed sales dataset
skewed_sales_df = sales_df.union(sales_df2)

# COMMAND ----------

display(skewed_sales_df.groupBy("customer_id").count().orderBy(col("customer_id")))

# COMMAND ----------

# DBTITLE 1,Write skewed sales dataframe to delta
skewed_sales_df.write.mode("overwrite").save("/tmp/tech-summit/de-opt/skew/skewed_sales_table")

# COMMAND ----------

# DBTITLE 1,Read skewed sales delta table 
skewed_sales_df = spark.read.format("delta").load("/tmp/tech-summit/de-opt/skew/skewed_sales_table")

# COMMAND ----------

# DBTITLE 1,Generate customer dimension dataset
data_rows = 2000 
column_count = 5
df_spec = (dg.DataGenerator(spark, name="cust_data", rows=data_rows,
                                                  partitions=4)
           .withColumn("customer_id", IntegerType(), minValue=1, maxValue=2000)
           .withColumn("c", FloatType(), 
                            expr="floor(rand() * 350) * (86400 + 3600)",
                            numColumns=column_count)
           )
                            
cust_df = df_spec.build()

# COMMAND ----------

display(cust_df)

# COMMAND ----------

# DBTITLE 1,Write customer dataframe to delta
cust_df.write.mode("overwrite").save("/tmp/tech-summit/de-opt/skew/cust_table")

# COMMAND ----------

# DBTITLE 1,Read customer delta table
cust_df = spark.read.format("delta").load("/tmp/tech-summit/de-opt/skew/cust_table")

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
join_df = skewed_sales_df.join(cust_df, skewed_sales_df.customer_id ==  cust_df.customer_id, "left")

# COMMAND ----------

# DBTITLE 1,Simulate write op with noop
join_df.write.format("noop").mode("overwrite").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join with AQE Skew Join Optimisation

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.sql.adaptive.skewJoin.enabled = true;

# COMMAND ----------

# DBTITLE 1,Simulate write op with noop
join_df.write.format("noop").mode("overwrite").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Broadcast Join

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Disable AQE skew join optimisation
# MAGIC set spark.sql.adaptive.skewJoin.enabled = false;
# MAGIC -- Enable spark & AQE broadcasts (threshold set to 100MB)
# MAGIC -- Don't forget to set the following conf in advance cluster confs
# MAGIC -- spark.driver.maxResultSize 8g
# MAGIC set spark.sql.autoBroadcastJoinThreshold = 104857600;
# MAGIC set spark.databricks.adaptive.autoBroadcastJoinThreshold = 104857600;

# COMMAND ----------

# DBTITLE 1,Simulate write op with noop
join_df.write.format("noop").mode("overwrite").save()

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC # Optimisations applied
# MAGIC * Pipeline metrics:
# MAGIC   * Before: 74 seconds
# MAGIC   * After: 3 seconds
# MAGIC * AQE skew join optimization
# MAGIC * Broadcast join

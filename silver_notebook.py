# Databricks notebook source
from pyspark.sql import SparkSession

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

full_load = True

# COMMAND ----------

spark = SparkSession.builder.appName("data-pipeline-spark").getOrCreate()
spark

# COMMAND ----------

# Preparação das tablelas bronze
database = "default"
spark.catalog.setCurrentDatabase(database)

tables = spark.catalog.listTables()
delta_tables = [table for table in tables if table.tableType == "MANAGED" and "bronze" in table.name]

if not full_load:
    delta_tables = [t for t in delta_tables if "new" in t.name]

customer_tables = [t for t in delta_tables if "customer" in t.name]
transaction_tables = [t for t in delta_tables if "transaction" in t.name]

def custom_sort_key(table):
    is_new = "new" in table.name.lower()
    return (is_new, table.name)
customer_tables = sorted(customer_tables, key=custom_sort_key)
transaction_tables = sorted(transaction_tables, key=custom_sort_key)


# COMMAND ----------

from delta.tables import *

# Função para realizar o merge (upsert) incremental
def upsert_to_delta(delta_table_name, source_df, merge_condition):
    if DeltaTable.isDeltaTable(spark, delta_table_name):
        delta_table = DeltaTable.forName(spark, delta_table_name)
        delta_table.alias("target").merge(
            source_df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        source_df.write.format("delta").mode("append").saveAsTable(delta_table_name)

# COMMAND ----------

for table in customer_tables:
    df = spark.read.format("delta").table(table.name)
    print(table.name)
    df.printSchema()

    df_silver_customer = df.dropDuplicates(["customer_id"]).withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))
    
    if full_load:
        if DeltaTable.isDeltaTable(spark, "silver_customer") and "new" in table.name:
            upsert_to_delta("silver_customer", df_silver_customer, "target.customer_id = source.customer_id")
        else:
            df_silver_customer.write.format("delta").mode("overwrite").saveAsTable("silver_customer")
    else:
        upsert_to_delta("silver_customer", df_silver_customer, "target.customer_id = source.customer_id")

# COMMAND ----------

for table in transaction_tables:
    df = spark.read.format("delta").table(table.name)
    print(table.name)
    df.printSchema()

    df_silver_transaction = df.dropDuplicates(["transaction_id"]).withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))

    if full_load:
        if DeltaTable.isDeltaTable(spark, "silver_transaction") and "new" in table.name:
            upsert_to_delta("silver_transaction", df_silver_transaction, "target.transaction_id = source.transaction_id")
        else:
            df_silver_transaction.write.format("delta").mode("overwrite").saveAsTable("silver_transaction")
    else:
        upsert_to_delta("silver_transaction", df_silver_transaction, "target.transaction_id = source.transaction_id")

# COMMAND ----------

print("\nTabelas criadas no catálogo:")
bronze_tables = spark.sql("SHOW TABLES LIKE 'silver_*'")
bronze_tables.show()
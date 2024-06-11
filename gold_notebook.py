# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

full_load = False

# COMMAND ----------

spark = SparkSession.builder.appName("data-pipeline-spark").getOrCreate()
spark

# COMMAND ----------

def read_silver_table(table_name):
    return spark.read.format("delta").table(table_name)

df_silver_customer = read_silver_table("silver_customer")
df_silver_transaction = read_silver_table("silver_transaction")

# COMMAND ----------

# Criando uma visão analítica com a soma das transações por cliente

# Dados consolidados de clientes (com dados de novas e antigas cargas)
df_gold_customer = df_silver_customer.unionByName(df_silver_customer).dropDuplicates(["customer_id"])

# Dados consolidados de transações (com dados de novas e antigas cargas)
df_gold_transaction = df_silver_transaction.unionByName(df_silver_transaction).dropDuplicates(["transaction_id"])

# Visão analítica com a soma das transações por cliente
from pyspark.sql.functions import sum

df_gold_aggregated_data = df_silver_transaction.groupBy("customer_id").agg(
    sum("amount").alias("total_amount_spent")
)


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

if full_load:
    # Carga Full: Sobrescreve as tabelas Gold com os dados mais recentes
    df_gold_customer.write.format("delta").mode("overwrite").saveAsTable("gold_customer")
    df_gold_transaction.write.format("delta").mode("overwrite").saveAsTable("gold_transaction")
    df_gold_aggregated_data.write.format("delta").mode("overwrite").saveAsTable("gold_aggregated_data")
else:
    # Carga Incremental: Realiza o merge (upsert) nos dados existentes
    upsert_to_delta("gold_customer", df_gold_customer, "target.customer_id  = source.customer_id")
    upsert_to_delta("gold_transaction", df_gold_transaction, "target.transaction_id = source.transaction_id")
    upsert_to_delta("gold_aggregated_data", df_gold_aggregated_data, "target.customer_id = source.customer_id")
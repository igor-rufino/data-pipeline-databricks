# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

LANDING_PATH = "/FileStore/data/raw/"

full_load = True
processing_date = date_trunc("second", current_timestamp())

# COMMAND ----------

spark = SparkSession.builder.appName("data-pipeline-spark").getOrCreate()
spark

# COMMAND ----------

files_and_dirs = dbutils.fs.ls(LANDING_PATH)
files = [item for item in files_and_dirs if not item.isDir()]

# Filtro para adicionar apenas os arquivos novos
if not full_load:
    files = [f for f in files if "new" in f.name]

# COMMAND ----------

table_count = 0

print("Lista de arquivos encontrados:")
for file in files:
    print(f"- {file.name}")
    file_stripped = file.name.split(".", 1)[0]
    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(LANDING_PATH + file.name)
        df = df.withColumn("processing_date", processing_date)
        df.printSchema()

        table_name = f"bronze_{file_stripped}"
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)

        if spark.catalog.tableExists(table_name):
            print(f"Tabela {table_name} criada com sucesso.\n")
            table_count += 1
        else:
            print(f"Erro ao criar a tabela {table_name}.\n")

    except Exception as e:
        print(f"Erro ao processar o arquivo {file.name}: {e}\n")

print(f"\nNúmero total de tabelas criadas: {table_count}")

# COMMAND ----------

print("\nTabelas criadas no catálogo:")
bronze_tables = spark.sql("SHOW TABLES LIKE 'bronze_*'")
bronze_tables.show()
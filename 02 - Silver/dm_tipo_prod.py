# Databricks notebook source
from pyspark.sql.functions import expr, current_timestamp, regexp_replace, col, udf, trim ,upper,max,count,desc
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, LongType, Row
from delta.tables import DeltaTable
import re

# COMMAND ----------

df_tipo_prod= spark.table('bronzedev.default.tiposprodutos')\
    .select(
        'COD_TIP_PRD',
        'COD_PRD',
        'NME_PRD',
        'DSC_PRD',
        'DSC_EMAIL',
        'DAT_INS',
        'DAT_ATU',
        )\
    .withColumn("NME_PRD", upper(trim(col("NME_PRD"))))\
    .withColumn("DSC_PRD", upper(trim(col("DSC_PRD"))))\
    .withColumn("DSC_EMAIL", upper(trim(col("DSC_EMAIL"))))\
    

# COMMAND ----------

df_de_para_TipoProd = spark.table('landingzonedadosdev.default.de_para_tipo_produto')\
    .select(
        'PRODUTO',
        'PRODUTO_AGREGAGADO'
    )\
.withColumn("PRODUTO", upper(trim(col("PRODUTO"))))\
.withColumn("PRODUTO_AGREGAGADO", upper(trim(col("PRODUTO_AGREGAGADO"))))\
.withColumnRenamed("PRODUTO", "NME_PRD")\
.withColumnRenamed("PRODUTO_AGREGAGADO", "NME_PRD_AGGR")

# COMMAND ----------

df_tipo_prod_comp = df_tipo_prod.join(df_de_para_TipoProd, ["NME_PRD"], "left")
df_tipo_prod_comp = df_tipo_prod_comp.na.fill("N/I")
df_tipo_prod_comp = df_tipo_prod_comp.na.replace("", "N/I")
df_tipo_prod_comp = df_tipo_prod_comp.drop_duplicates()

# COMMAND ----------

schema = StructType([
  StructField('COD_TIP_PRD', StringType()),
  StructField('COD_PRD', StringType()),
  StructField('NME_PRD', StringType()),
  StructField('DSC_PRD', StringType()),
  StructField('DSC_EMAIL', StringType()),
  StructField('DAT_INS', StringType()),
  StructField('DAT_ATU', StringType()),
  StructField('NME_PRD_AGGR', StringType())
])

novolinha = {
    'NME_PRD': '-1',
    'COD_TIP_PRD': '-1',
    'COD_PRD': '-1',
    'DSC_PRD': '-1',
    'DSC_EMAIL': '-1',
    'DAT_INS': '1900-01-01 00:00:00.000',
    'DAT_ATU': '1900-01-01 00:00:34.000',
    'NME_PRD_AGGR': '-1'
}
df = spark.createDataFrame([novolinha], schema)
df_tipo_prod_comp = df_tipo_prod_comp.union(df)


# COMMAND ----------

df_tipo_prod_comp.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("silverdev.default.dm_tip_prd")

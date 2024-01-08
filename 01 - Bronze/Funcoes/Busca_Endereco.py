# Databricks notebook source
# MAGIC %md
# MAGIC # Validação Endereço Correios

# COMMAND ----------

from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import col, lit, udf, when
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

# COMMAND ----------

def buscar_cep_logradouro(df: DataFrame, cep_col_name: str) -> DataFrame:
    # Realiza a busca nas tabelas de logradouro, localidade e bairro
    logradouro = spark.table("landingzonedadosdev.correios.LOG_LOGRADOURO")
    localidade = spark.table("landingzonedadosdev.correios.LOG_LOCALIDADE")
    bairro = spark.table("landingzonedadosdev.correios.LOG_BAIRRO")

    df_joined = df.join(logradouro, col(cep_col_name) == logradouro["CEP"], "left_outer") \
                  .join(localidade, logradouro["LOC_NU"] == localidade["LOC_NU"], "left_outer") \
                  .join(bairro, logradouro["BAI_NU_INI"] == bairro["BAI_NU"], "left_outer") \
                  .select(
                      df["*"],
                      bairro["BAI_NO"].alias("BAI_NO_BAI"),
                      localidade["LOC_NO"].alias("LOC_NO_LOC"),
                      logradouro["LOG_NU"].alias("LOG_NU_LOG"),
                      logradouro["UFE_SG"].alias("UFE_SG_LOG"),
                      logradouro["BAI_NU_INI"].alias("BAI_NU_INI_LOG"),
                      logradouro["BAI_NU_FIM"].alias("BAI_NU_FIM_LOG"),
                      logradouro["LOG_NO"].alias("LOG_NO_LOG"),
                      logradouro["LOG_COMPLEMENTO"].alias("LOG_COMPLEMENTO_LOG"),
                      logradouro["CEP"].alias("CEP_LOG"),
                      logradouro["TLO_TX"].alias("TLO_TX_LOG"),
                      logradouro["LOG_STA_TLO"].alias("LOG_STA_TLO_LOG"),
                      logradouro["LOG_NO_ABREV"].alias("LOG_NO_ABREV_LOG")
                  )
    return df_joined

# A função validar_cep chama a função buscar_cep_logradouro e retorna o resultado
def validar_cep(df: DataFrame, cep_col_name: str) -> DataFrame:
    df = buscar_cep_logradouro(df, cep_col_name)
    return df

# COMMAND ----------

temp_view_name = dbutils.widgets.get("temp_view_name")
df = spark.read.table(f"global_temp.{temp_view_name}")

cep_col_name = "CEP_Limpo"
df_final = validar_cep(df, cep_col_name)

# COMMAND ----------

df_final.createOrReplaceGlobalTempView(f"{temp_view_name}_corrigido")

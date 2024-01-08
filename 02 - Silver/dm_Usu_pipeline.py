# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import expr, regexp_replace, row_number, lit, udf, expr, current_timestamp, col, to_date
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from pyspark.sql import DataFrame, Row
import re
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Validar CPF
def validar_cpf(cpf):
    cpf = [int(char) for char in cpf if char.isdigit()]
    
    if len(cpf) != 11 or len(set(cpf)) == 1:
        return 0

    for i in range(9, 11):
        valor = sum((cpf[num] * ((i+1) - num) for num in range(0, i)))
        digito = ((valor * 10) % 11) % 10
        if digito != cpf[i]:
            return 0
    return 1

# COMMAND ----------

# DBTITLE 1,Validar CEP
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

# DBTITLE 1,Truncate da dimensão dm_Usu
truncate_Usuario_df = spark.sql("truncate table silverdev.default.dm_usu")

# COMMAND ----------

# DBTITLE 1,Insert da dimensão dm_Usu
insert_Usuario_df = spark.sql("select COD_PRP_PF, ifnull(upper(trim(NUM_CPF)), 'N/I') as NUM_CPF, cast(ifnull(DAT_NASC, '1900-01-01') as date) as DAT_NASC, case when upper(trim(DSC_NAC)) in ('BRASILEIRA', 'BRASIL') then 'BRASIL' else 'N/I' end as DSC_NAC, case when (NUM_DEP is null or NUM_DEP = 0) then '-1' else NUM_DEP end as NUM_DEP, case when upper(trim(DSC_EST_CIV)) in ('SO', 'CA', 'DI') then upper(trim(DSC_EST_CIV)) else 'N/I' end as DSC_EST_CIV, case when upper(trim(DSC_SEX)) in ('F','M') then upper(trim(DSC_SEX)) else 'N/I' end as DSC_SEX, ifnull(upper(trim(DSC_NAT)), 'N/I') as DSC_NAT, case when upper(trim(DSC_EML)) = '' then 'N/I' when upper(trim(DSC_EML)) is not null then upper(trim(DSC_EML)) else 'N/I' end as DSC_EML, ifnull(upper(trim(DSC_END)), 'N/I') as DSC_END, ifnull(upper(trim(DSC_BRRO)), 'N/I') as DSC_BRRO, ifnull(upper(trim(DSC_CID)), 'N/I') as DSC_CID, ifnull(upper(trim(DSC_EST)), 'N/I') as DSC_EST, ifnull(upper(trim(NUM_CEP)), 'N/I') as NUM_CEP, ifnull(upper(trim(DSC_TIP_RES)), 'N/I') as DSC_TIP_RES, ifnull(upper(trim(NUM_TEL01)), '-1') as NUM_TEL01, ifnull(upper(trim(TIP_TEL01)), 'N/I') as TIP_TEL01, ifnull(upper(trim(NUM_TEL02)), '-1') as NUM_TEL02, ifnull(upper(trim(TIP_TEL02)), 'N/I') as TIP_TEL02 from bronzedev.default.propostaspf")

# COMMAND ----------

# DBTITLE 1,Retira caracteres
insert_Usuario_df = insert_Usuario_df.withColumn("NUM_CPF", regexp_replace("NUM_CPF", "[./-]", ""))
insert_Usuario_df = insert_Usuario_df.withColumn("NUM_CEP", regexp_replace("NUM_CEP", "[./-]", ""))
insert_Usuario_df = insert_Usuario_df.withColumn("NUM_TEL01", regexp_replace("NUM_TEL01", "[./-]", ""))
insert_Usuario_df = insert_Usuario_df.withColumn("NUM_TEL02", regexp_replace("NUM_TEL02", "[./-]", ""))
validar_cpf_udf = udf(validar_cpf, IntegerType())
insert_Usuario_df = insert_Usuario_df.withColumn("FLG_CPF_VAL", validar_cpf_udf(insert_Usuario_df["NUM_CPF"]))
insert_Usuario_df = insert_Usuario_df.withColumn("CEP_LIMP", col("NUM_CEP"))
cep_col_name = "NUM_CEP"
insert_Usuario_df = validar_cep(insert_Usuario_df, cep_col_name)

# COMMAND ----------

# DBTITLE 1,Add coluna PK
x = Window().orderBy(lit('A'))
insert_Usuario_df = insert_Usuario_df.withColumn("SRK_USUARIO", row_number().over(x))

# COMMAND ----------

# DBTITLE 1,Escreve na camada Silver
insert_Usuario_df.write.insertInto("silverdev.default.dm_Usu", overwrite=True)

# COMMAND ----------

# DBTITLE 1,Insere registro não informado
nf_Usuario_df = spark.sql("select -1 as COD_PRP_PF, 'N/I' as NUM_CPF, cast('1900-01-01' as date) as DAT_NASC, 'N/I' as DSC_NAC, -1 as NUM_DEP, 'N/I' as DSC_EST_CIV, 'N/I' as DSC_SEX, 'N/I' as DSC_NAT, 'N/I' as DSC_EML, 'N/I' as DSC_END, 'N/I' as DSC_BRRO, 'N/I' as DSC_CID, 'N/I' as DSC_EST, 'N/I' as NUM_CEP, 'N/I' as DSC_TIP_RES, '-1' as NUM_TEL01, 'N/I' as TIP_TEL01, '-1' as NUM_TEL02, 'N/I' as TIP_TEL02, -1 as FLG_CPF_VAL, 'N/I' as CEP_LIMP, 'N/I' as BAI_NO_BAI, 'N/I' as LOC_NO_LOC, 'N/I' as LOG_NU_LOG, 'N/I' as UFE_SG_LOG, 'N/I' as BAI_NU_INI_LOG, 'N/I' as BAI_NU_FIM_LOG, 'N/I' as LOG_NO_LOG, 'N/I' as LOG_COMPLEMENTO_LOG, 'N/I' as CEP_LOG, 'N/I' as TLO_TX_LOG, 'N/I' as LOG_STA_TLO_LOG, 'N/I' as LOG_NO_ABREV_LOG, -1 as SRK_USUARIO")

# COMMAND ----------

# DBTITLE 1,Escreve na camada Silver
nf_Usuario_df.write.insertInto("silverdev.default.dm_Usu", overwrite=False)

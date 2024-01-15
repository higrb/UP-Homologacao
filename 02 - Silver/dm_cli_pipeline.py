# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import expr, regexp_replace, row_number, lit
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# COMMAND ----------

# DBTITLE 1,Truncate da dimensão dm_Cli
truncate_Entidade_df = spark.sql("truncate table silverdev.default.dm_Cli")

# COMMAND ----------

# DBTITLE 1,Insert da dimensão dm_Cli
insert_Entidade_df = spark.sql("""
    select
    COD_ENT,
    COD_TIP_ENT,
    ifnull(upper(trim(NME_RAZ_SOC)), 'N/I') as NME_ENT,
    ifnull(upper(trim(COD_STA_TRN)), 'N/I') as COD_STA_TRN,
    ifnull(upper(trim(NUM_CNPJ_LIMPO)), 'N/I') as NUM_CNPJ,
    ifnull(upper(trim(NUM_CEP_LIMPO)), 'N/I') as NUM_CEP,
    ifnull(upper(trim(NUM_TEL)), 'N/I') as NUM_TEL,
    ifnull(upper(trim(NME_CONT)), 'N/I') as NME_CONT,
    ifnull(upper(trim(DSC_EMAIL)), 'N/I') as DSC_EMAIL,
    FLG_CNPJ_VAL,
    ifnull(upper(trim(DSC_TIPO_DOC)), 'N/I') as DSC_TIPO_DOC,
    ifnull(upper(trim(LOG_NO_LOG)), 'N/I') as LOG_NO_LOG,
    ifnull(upper(trim(NME_BRROI_VAL)), 'N/I') as NME_BRROI_VAL,
    ifnull(upper(trim(NME_CID_VAL)), 'N/I') as NME_CID_VAL,
    ifnull(upper(trim(LOG_COPTO_LOG_VAL)), 'N/I') as LOG_COPTO_LOG_VAL,
    ifnull(upper(trim(DSC_UF_VAL)), 'N/I') as DSC_UF_VAL,
    ifnull(upper(trim(CEP_LOG_VAL)), 'N/I') as CEP_LOG_VAL
    from
    bronzedev.default.entidades
""")

# COMMAND ----------

# DBTITLE 1,Add coluna PK
x = Window().orderBy(lit('A'))
insert_Entidade_df = insert_Entidade_df.withColumn("SRK_CLI", row_number().over(x))

# COMMAND ----------

# DBTITLE 1,Escreve na camada Silver
insert_Entidade_df.write.insertInto("silverdev.default.dm_Cli", overwrite=True)

# COMMAND ----------

# DBTITLE 1,Insere registro não informado
nf_Entidade_df = spark.sql("""
    select
    -1 as COD_ENT,
    -1 as COD_TIP_ENT,
    'N/I' as NME_ENT,
    'N/I' as COD_STA_TRN,
    'N/I' as NUM_CNPJ,
    'N/I' as NUM_CEP,
    'N/I' as NUM_TEL,
    'N/I' as NME_CONT,
    'N/I' as DSC_EMAIL,
    0 as FLG_CNPJ_VAL,
    'N/I' as DSC_TIPO_DOC,
    'N/I' as LOG_NO_LOG,
    'N/I' as NME_BRROI_VAL,
    'N/I' as NME_CID_VAL,
    'N/I' as LOG_COPTO_LOG_VAL,
    'N/I' as DSC_UF_VAL,
    'N/I' as CEP_LOG_VAL,
    -1 as SRK_CLI
""")

# COMMAND ----------

# DBTITLE 1,Escreve na camada Silver
nf_Entidade_df.write.insertInto("silverdev.default.dm_Cli", overwrite=False)

# Databricks notebook source
# MAGIC %md
# MAGIC # Tabela ContasUsuarios

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import expr, current_timestamp, regexp_replace, col

# COMMAND ----------

# Foi adicionado uma etapa de transformação para selecionar um subconjunto de colunas na tabela landingzonedadosdev.default.ContasUsuarios e renomeá-las para nomes mais legíveis e intuitivos, respeitando os mnemonicos conforme documentação.
df_ContasUsuarios = spark.table('landingzonedadosdev.default.ContasUsuarios')\
    .select(
        'CntUsrCodigo'
        ,'PrpCodigo'
        ,'DiaVencimento'
        ,'LimiteCredito'
        ,'CreditoDisponivel'
        ,'CreditoParcelamento'
        ,'DataCriacao'
        ,'Status'
        ,'Numero'
        ,'Sequencial'
        ,'NivelBloqueio'
        ,'PrdCodigo'
        ,'DiaCorte'
        ,'DiaExpiracao'
        ,'PermiteVendaParcelada'
        ,'ValorCarga'
        ,'data_insercao'
        ,'data_atualizacao'
        )\
    .withColumnRenamed("CntUsrCodigo", "COD_CNT_USR")\
    .withColumnRenamed("PrpCodigo", "COD_PRP")\
    .withColumnRenamed("DiaVencimento", "NUM_DIA_VENC")\
    .withColumnRenamed("LimiteCredito", "VAL_LIM_CRD")\
    .withColumnRenamed("CreditoDisponivel", "VAL_CRD_DISP")\
    .withColumnRenamed("CreditoParcelamento", "VAL_CRD_PARC")\
    .withColumnRenamed("DataCriacao", "DAT_CRIAC")\
    .withColumnRenamed("Status", "COD_STA_TRN")\
    .withColumnRenamed("Numero", "NUM_CTR")\
    .withColumnRenamed("Sequencial", "NUM_SEQ_PRP")\
    .withColumnRenamed("NivelBloqueio", "COD_NIV_BLOQ")\
    .withColumnRenamed("PrdCodigo", "COD_PRD")\
    .withColumnRenamed("DiaCorte", "NUM_DIA_CORTE")\
    .withColumnRenamed("DiaExpiracao", "NUM_DIA_EXP")\
    .withColumnRenamed("PermiteVendaParcelada", "FLG_PERM_VEND_PARC")\
    .withColumnRenamed("ValorCarga", "VAL_CARG")\
        .withColumnRenamed("data_insercao", "DATA_INSCE")\
    .withColumnRenamed("data_atualizacao", "DATA_ATLC")

# COMMAND ----------

# Carregar a tabela Delta de destino como DeltaTable pelo nome do catálogo
deltaTable = DeltaTable.forName(spark, "bronzedev.default.ContasUsuarios")

# Realizar o MERGE
(deltaTable.alias("dest")
 .merge(
     df_ContasUsuarios.alias("src"),
     "dest.COD_CNT_USR = src.COD_CNT_USR"
 )
 .whenMatchedUpdateAll()   # Atualiza todas as colunas correspondentes
 .whenNotMatchedInsertAll() # Insere novas linhas que não correspondem
 .execute()
)

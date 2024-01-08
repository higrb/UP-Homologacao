# Databricks notebook source
# MAGIC %md
# MAGIC # Tabela TiposProdutos

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import expr, current_timestamp, regexp_replace, col

# COMMAND ----------

# Foi adicionado uma etapa de transformação para selecionar um subconjunto de colunas na tabela landingzonedadosdev.default.TiposProdutos e renomeá-las para nomes mais legíveis e intuitivos, respeitando os mnemonicos conforme documentação.
df_TiposProdutos = spark.table('landingzonedadosdev.default.TiposProdutos')\
    .select(
        'TpoPrdCodigo'
        ,'PrdCodigo'
        ,'Nome'
        ,'Descricao'
        ,'Email'
        ,'data_insercao'
        ,'data_atualizacao'
        )\
    .withColumnRenamed("TpoPrdCodigo", "COD_TIP_PRD")\
    .withColumnRenamed("PrdCodigo", "COD_PRD")\
    .withColumnRenamed("Nome", "NME_PRD")\
    .withColumnRenamed("Descricao", "DSC_PRD")\
    .withColumnRenamed("Email", "DSC_EMAIL")\
    .withColumnRenamed("data_insercao", "DATA_INSCE")\
    .withColumnRenamed("data_atualizacao", "DATA_ATLC")

# COMMAND ----------

# Carregar a tabela Delta de destino como DeltaTable pelo nome do catálogo
deltaTable = DeltaTable.forName(spark, "bronzedev.default.TiposProdutos")

# Realizar o MERGE
(deltaTable.alias("dest")
 .merge(
     df_TiposProdutos.alias("src"),
     "dest.COD_TIP_PRD = src.COD_TIP_PRD"
 )
 .whenMatchedUpdateAll()   # Atualiza todas as colunas correspondentes
 .whenNotMatchedInsertAll() # Insere novas linhas que não correspondem
 .execute()
)

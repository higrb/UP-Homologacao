# Databricks notebook source
# MAGIC %md
# MAGIC # Tabela Propostas

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import expr, current_timestamp, regexp_replace, col

# COMMAND ----------

# Foi adicionado uma etapa de transformação para selecionar um subconjunto de colunas na tabela landingzonedadosdev.default.Propostas e renomeá-las para nomes mais legíveis e intuitivos, respeitando os mnemonicos conforme documentação.
df_Propostas = spark.table('landingzonedadosdev.default.Propostas')\
    .select(
        'PrpCodigo'
        ,'Nome'
        ,'TipoCadastro'
        ,'NomeCartao'
        ,'Data'
        ,'EntCodigo'
        ,'StatusSGP'
        ,'data_insercao'
        ,'data_atualizacao'
        )\
    .withColumnRenamed("PrpCodigo", "COD_PRP")\
    .withColumnRenamed("Nome", "NME_PRP_CLI")\
    .withColumnRenamed("TipoCadastro", "TIP_CAD")\
    .withColumnRenamed("NomeCartao", "NME_CTO")\
    .withColumnRenamed("Data", "DAT_PRP")\
    .withColumnRenamed("EntCodigo", "COD_ENT")\
    .withColumnRenamed("StatusSGP", "COD_STA_SGP")\
    .withColumnRenamed("data_insercao", "DATA_INSCE")\
    .withColumnRenamed("data_atualizacao", "DATA_ATLC")

# COMMAND ----------

# Carregar a tabela Delta de destino como DeltaTable pelo nome do catálogo
deltaTable = DeltaTable.forName(spark, "bronzedev.default.Propostas")

# Realizar o MERGE
(deltaTable.alias("dest")
 .merge(
     df_Propostas.alias("src"),
     "dest.COD_PRP = src.COD_PRP"
 )
 .whenMatchedUpdateAll()   # Atualiza todas as colunas correspondentes
 .whenNotMatchedInsertAll() # Insere novas linhas que não correspondem
 .execute()
)

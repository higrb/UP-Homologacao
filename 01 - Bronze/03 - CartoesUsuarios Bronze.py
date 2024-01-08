# Databricks notebook source
# MAGIC %md
# MAGIC # Tabela CartoesUsuarios

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import expr, current_timestamp, regexp_replace, col, udf
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
import re

# COMMAND ----------

# Função para identificar se o campo é CPF ou CNPJ
def identificar_campo(campo):
    try:
        campo = re.sub('[^0-9]', '', campo)
        documento = [int(char) for char in campo if char.isdigit()]
        
        if len(campo) == 11:
            
            for i in range(9, 11):
                valor = sum((documento[num] * ((i+1) - num) for num in range(0, i)))
                digito = ((valor * 10) % 11) % 10        
            if digito != documento[i]:
                return 0

        elif len(campo) == 14:
            
            validadores = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
            validadores = [6] + validadores
            for i in range(12, 14):
                valor = sum((documento[num] * validadores[num]) for num in range(0, i))
                digito = 11 - (valor % 11)
                if digito >= 10:
                    digito = 0
                if digito != documento[i]:
                    return 0       
        else:
            return 0
        
        return 1
    except Exception as e:
        return 0


# Função para identificar se o campo é CPF ou CNPJ
def identificar_Tipo_campo(campo):
    try:
        campo = re.sub('[^0-9]', '', campo)
        if len(campo) == 11:
            return 'CPF'
        elif len(campo) == 14:
            return 'CNPJ'
        else:
            return '0'
    except Exception as e:
        return 'N/I'

identificar_campo_udf = udf(identificar_campo, IntegerType())
identificar_Tipo_campo_udf = udf(identificar_Tipo_campo, StringType())

# COMMAND ----------

# Foi adicionado uma etapa de transformação para selecionar um subconjunto de colunas na tabela landingzonedadosdev.default.CartoesUsuarios e renomeá-las para nomes mais legíveis e intuitivos, respeitando os mnemonicos conforme documentação.
df_CartoesUsuarios = spark.table('landingzonedadosdev.default.CartoesUsuarios')\
    .select(
        'CrtUsrCodigo'
        ,'TpoPrdCodigo'
        ,'CntUsrCodigo'
        ,'Tipo'
        ,'Numero'
        ,'CPF'
        ,'Nome'
        ,'Sexo'
        ,'DataNascimento'
        ,'DataEmissao'
        ,'DataVencimento'
        ,'Status'
        )\
        .withColumnRenamed("CrtUsrCodigo","COD_CRT_USR")\
        .withColumnRenamed("TpoPrdCodigo","COD_TIP_PRD")\
        .withColumnRenamed("CntUsrCodigo","COD_CTA_USR")\
        .withColumnRenamed("Tipo","COD_TIP_CTR")\
        .withColumnRenamed("Numero","NUM_CRT")\
        .withColumnRenamed("CPF","NUM_CPF")\
        .withColumnRenamed("Nome","NME_USR_EMB")\
        .withColumnRenamed("Sexo","COD_TIP_SEX")\
        .withColumnRenamed("DataNascimento","DAT_NASC_USR")\
        .withColumnRenamed("DataEmissao","DAT_EMIS_CTO")\
        .withColumnRenamed("DataVencimento","DAT_VENC_CTO")\
        .withColumnRenamed("Status","COD_STA_TRN")\
        .withColumnRenamed("data_insercao", "DATA_INSCE")\
        .withColumnRenamed("data_atualizacao", "DATA_ATLC")

# COMMAND ----------

# VALIDA CNPJ
df_CartoesUsuarios = df_CartoesUsuarios.withColumn("FLG_CPF_VAL", identificar_campo_udf(df_CartoesUsuarios["NUM_CPF"]))

# COMMAND ----------

# DATA CLEAN CAMPO CNPJ/CEP
df_CartoesUsuarios = df_CartoesUsuarios.withColumn("NUM_CPF_LIMPO",regexp_replace(col("NUM_CPF").cast("string"), "[^0-9]", ""))
df_CartoesUsuarios = df_CartoesUsuarios.withColumn("TIP_DOC", identificar_Tipo_campo_udf(df_CartoesUsuarios["NUM_CPF_LIMPO"]))

# COMMAND ----------

# Carregar a tabela Delta de destino como DeltaTable pelo nome do catálogo
deltaTable = DeltaTable.forName(spark, "bronzedev.default.CartoesUsuarios")

# Realizar o MERGE
(deltaTable.alias("dest")
 .merge(
     df_CartoesUsuarios.alias("src"),
     "dest.COD_CRT_USR = src.COD_CRT_USR"
 )
 .whenMatchedUpdateAll()   # Atualiza todas as colunas correspondentes
 .whenNotMatchedInsertAll() # Insere novas linhas que não correspondem
 .execute()
)

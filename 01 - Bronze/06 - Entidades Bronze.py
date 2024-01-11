# Databricks notebook source
# MAGIC %md
# MAGIC # Tabela Entidades

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

# Foi adicionado uma etapa de transformação para selecionar um subconjunto de colunas na tabela landingzonedadosdev.default.ContasUsuarios e renomeá-las para nomes mais legíveis e intuitivos, respeitando os mnemonicos conforme documentação.
df_Entidades = spark.table('landingzonedadosdev.default.Entidades')\
    .select(
        'EntCodigo'
        ,'TpoEntCodigo'
        ,'Nome'
        ,'RazaoSocial'
        ,'Status'
        ,'CNPJ'
        ,'Cidade'
        ,'Estado'
        ,'Endereco'
        ,'Bairro'
        ,'CEP'
        ,'Telefone'
        ,'Contato'
        ,'Email'
        ,'data_insercao'
        ,'data_atualizacao'
        )\
        .withColumnRenamed("EntCodigo", "COD_ENT")\
        .withColumnRenamed("TpoEntCodigo", "COD_TIP_ENT")\
        .withColumnRenamed("Nome", "NME_ENT")\
        .withColumnRenamed("RazaoSocial", "NME_RAZ_SOC")\
        .withColumnRenamed("Status", "COD_STA_TRN")\
        .withColumnRenamed("CNPJ", "NUM_CNPJ")\
        .withColumnRenamed("Cidade", "NME_CID")\
        .withColumnRenamed("Estado", "NME_EST")\
        .withColumnRenamed("Endereco", "DSC_END")\
        .withColumnRenamed("Bairro", "NME_BRRO")\
        .withColumnRenamed("CEP", "NUM_CEP")\
        .withColumnRenamed("Telefone", "NUM_TEL")\
        .withColumnRenamed("Contato", "NME_CONT")\
        .withColumnRenamed("Email", "DSC_EMAIL")\
        .withColumnRenamed("data_insercao", "DATA_INSCE")\
        .withColumnRenamed("data_atualizacao", "DATA_ATLC")

# COMMAND ----------

# VALIDA CNPJ
df_Entidades = df_Entidades.withColumn("FLG_CNPJ_VAL", identificar_campo_udf(df_Entidades["NUM_CNPJ"]))

# COMMAND ----------

# DATA CLEAN CAMPO CNPJ/CEP
df_Entidades = df_Entidades.withColumn("NUM_CNPJ_LIMPO",regexp_replace(col("NUM_CNPJ").cast("string"), "[^0-9]", ""))
df_Entidades = df_Entidades.withColumn("TIPO_DOCUMENTO", identificar_Tipo_campo_udf(df_Entidades["NUM_CNPJ_LIMPO"]))
df_Entidades = df_Entidades.withColumn("CEP_Limpo", regexp_replace(col("NUM_CEP"), "[^\\d]", ""))

# COMMAND ----------

df_Entidades.createOrReplaceGlobalTempView("Entidades_temp_view")

result_status = dbutils.notebook.run(
    "/Users/higor.borges@upbrasil.com/01 - Bronze/Funcoes/Busca_Endereco",
    timeout_seconds=600,
    arguments={"temp_view_name": "Entidades_temp_view"}
)

# COMMAND ----------

df_Entidades_final = spark.sql("""
                    SELECT
                        COD_ENT
                        ,COD_TIP_ENT
                        ,NME_ENT
                        ,NME_RAZ_SOC
                        ,COD_STA_TRN
                        ,NUM_CNPJ
                        ,NME_CID
                        ,NME_EST
                        ,DSC_END
                        ,NME_BRRO
                        ,NUM_CEP
                        ,NUM_TEL
                        ,NME_CONT
                        ,DSC_EMAIL
                        ,DATA_INSCE
                        ,DATA_ATLC
                        ,FLG_CNPJ_VAL
                        ,TIPO_DOCUMENTO AS DSC_TIPO_DOC
                        ,NUM_CNPJ_LIMPO
                        ,CEP_Limpo AS NUM_CEP_LIMPO
                        ,LOG_NO_LOG AS LOG_NO_LOG
                        ,BAI_NO_BAI AS NME_BRROI_VAL
                        ,LOC_NO_LOC AS NME_CID_VAL
                        ,LOG_COMPLEMENTO_LOG AS LOG_COPTO_LOG_VAL
                        ,LOG_NO_ABREV_log AS LOG_NO_ABREV_log_VAL
                        ,UFE_SG_LOG AS DSC_UF_VAL
                        ,CEP_LOG AS CEP_LOG_VAL
                    from global_temp.Entidades_temp_view_corrigido
""")

# COMMAND ----------

# Carregar a tabela Delta de destino como DeltaTable pelo nome do catálogo
deltaTable = DeltaTable.forName(spark, "bronzedev.default.entidades")

# Realizar o MERGE
(deltaTable.alias("dest")
 .merge(
     df_Entidades_final.alias("src"),
     "dest.COD_ENT = src.COD_ENT"
 )
 .whenMatchedUpdateAll()   # Atualiza todas as colunas correspondentes
 .whenNotMatchedInsertAll() # Insere novas linhas que não correspondem
 .execute()
)

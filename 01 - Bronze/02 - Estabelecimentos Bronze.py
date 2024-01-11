# Databricks notebook source
# MAGIC %md
# MAGIC # Tabela Estabelecimentos

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

df_estabelecimentos = spark.table('landingzonedadosdev.default.estabelecimentos')\
    .select(
        'EstCodigo',
        'Nome',
        'RazaoSocial',
        'CNPJ',
        'CNPJMatriz',
        'Cidade',
        'Estado',
        'Endereco',
        'Bairro',
        'CEP',
        'Telefone',
        'Email',
        'SocioProprietario',
        'CPFSocioProprietario',
        'Agencia',
        'ContaCorrente',
        'Data',
        'DataValidacaoCNPJ',
        'data_insercao',
        'data_atualizacao'
    )\
    .withColumnRenamed("EstCodigo", "COD_ESTAB")\
    .withColumnRenamed("Nome", "NME_ESTAB")\
    .withColumnRenamed("RazaoSocial", "NME_RAZ_SOC")\
    .withColumnRenamed("CNPJ", "NUM_CNPJ")\
    .withColumnRenamed("CNPJMatriz", "NUM_CNPJ_MTRZ")\
    .withColumnRenamed("Cidade", "NME_CID")\
    .withColumnRenamed("Estado", "NME_EST")\
    .withColumnRenamed("Endereco", "NME_END")\
    .withColumnRenamed("Bairro", "NME_BRRO")\
    .withColumnRenamed("CEP", "NUM_CEP")\
    .withColumnRenamed("Telefone", "NUM_TEL")\
    .withColumnRenamed("Email", "DSC_EMAIL")\
    .withColumnRenamed("SocioProprietario", "NME_SOC_PRP")\
    .withColumnRenamed("CPFSocioProprietario", "NUM_CPF_SOC_PRP")\
    .withColumnRenamed("Agencia", "NUM_AGC")\
    .withColumnRenamed("ContaCorrente", "NUM_CTA_CORR")\
    .withColumnRenamed("Data", "DAT_CAD_ESTAB")\
    .withColumnRenamed("DataValidacaoCNPJ", "DAT_VAL_CNPJ")\
    .withColumnRenamed("data_insercao", "DATA_INSCE")\
    .withColumnRenamed("data_atualizacao", "DATA_ATLC")

# COMMAND ----------

# VALIDA CNPJ
df_estabelecimentos = df_estabelecimentos.withColumn("FLG_CNPJ_VAL", identificar_campo_udf(df_estabelecimentos["NUM_CNPJ"]))
df_estabelecimentos = df_estabelecimentos.withColumn("FLG_CNPJ_VAL_MAT", identificar_campo_udf(df_estabelecimentos["NUM_CNPJ_MTRZ"]))

# COMMAND ----------

# DATA CLEAN CAMPO CNPJ/CEP
df_estabelecimentos = df_estabelecimentos.withColumn("NUM_CNPJ_LIMPO",regexp_replace(col("NUM_CNPJ").cast("string"), "[^0-9]", ""))
df_estabelecimentos = df_estabelecimentos.withColumn("NUM_CNPJ_LIMPO_MAT",regexp_replace(col("NUM_CNPJ_MTRZ").cast("string"), "[^0-9]", ""))
df_estabelecimentos = df_estabelecimentos.withColumn("TIP_DOC", identificar_Tipo_campo_udf(df_estabelecimentos["NUM_CNPJ_LIMPO"]))
df_estabelecimentos = df_estabelecimentos.withColumn("TIP_DOC_MAT", identificar_Tipo_campo_udf(df_estabelecimentos["NUM_CNPJ_LIMPO_MAT"]))
df_estabelecimentos = df_estabelecimentos.withColumn("CEP_Limpo", regexp_replace(col("NUM_CEP"), "[^\\d]", ""))

# COMMAND ----------

df_estabelecimentos.createOrReplaceGlobalTempView("estabelecimento_temp_view")

result_status = dbutils.notebook.run(
    "/Users/higor.borges@upbrasil.com/01 - Bronze/Funcoes/Busca_Endereco",
    timeout_seconds=600,
    arguments={"temp_view_name": "estabelecimento_temp_view"}
)

# COMMAND ----------

df_estabelecimento_final = spark.sql("""
                    SELECT
                        COD_ESTAB
                        ,NME_ESTAB
                        ,NME_RAZ_SOC
                        ,NUM_CNPJ
                        ,NUM_CNPJ_MTRZ
                        ,NME_CID
                        ,NME_EST
                        ,NME_END
                        ,NME_BRRO
                        ,NUM_CEP
                        ,NUM_TEL
                        ,DSC_EMAIL
                        ,NME_SOC_PRP
                        ,NUM_CPF_SOC_PRP
                        ,NUM_AGC
                        ,NUM_CTA_CORR
                        ,DAT_CAD_ESTAB
                        ,DAT_VAL_CNPJ
                        ,DATA_INSCE
                        ,DATA_ATLC
                        ,NUM_CNPJ_LIMPO
                        ,NUM_CNPJ_LIMPO_MAT
                        ,LOG_STA_TLO_LOG
                        ,TIP_DOC AS DSC_TIPO_DOC
                        ,TIP_DOC_MAT AS DSC_TIPO_DOC_MAT
                        ,CEP_Limpo AS NUM_CEP_LIMPO
                        ,LOG_NO_LOG AS NME_LOG_VAL
                        ,BAI_NO_BAI AS NME_BAI_VAL
                        ,LOC_NO_LOC AS NME_CID_VAL
                        ,LOG_COMPLEMENTO_LOG AS LOG_COPTO_LOG_VAL
                        ,LOG_NO_ABREV_log AS NME_ABV_LOG_VAL
                        ,UFE_SG_LOG AS NME_UF_VAL
                        ,CEP_LOG AS CEP_VAL
                        ,FLG_CNPJ_VAL
                        ,FLG_CNPJ_VAL_MAT
                    from global_temp.estabelecimento_temp_view_corrigido
""")

# COMMAND ----------

df_estabelecimento_final.write.mode("overwrite").saveAsTable('bronzedev.default.estabelecimentos')

# COMMAND ----------

# Carregar a tabela Delta de destino como DeltaTable pelo nome do catálogo
deltaTable = DeltaTable.forName(spark, "bronzedev.default.estabelecimentos")

# Realizar o MERGE
(deltaTable.alias("dest")
 .merge(
     df_estabelecimento_final.alias("src"),
     "dest.Cod_Estab = src.Cod_Estab"
 )
 .whenMatchedUpdateAll()   # Atualiza todas as colunas correspondentes
 .whenNotMatchedInsertAll() # Insere novas linhas que não correspondem
 .execute()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from bronzedev.default.transacao t
# MAGIC inner join bronzedev.default.estabelecimentos et on et.COD_ESTAB = t.COD_ESTAB

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from bronzedev.default.estabelecimentos--279.757 --279.785

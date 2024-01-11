# Databricks notebook source
# MAGIC %md
# MAGIC # Tabela Propostas PF

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

# Foi adicionado uma etapa de transformação para selecionar um subconjunto de colunas na tabela landingzonedadosdev.default.Propostas e renomeá-las para nomes mais legíveis e intuitivos, respeitando os mnemonicos conforme documentação.
df_Propostaspf = spark.table('landingzonedadosdev.default.propostaspf')\
    .select(
        'PrpPFCodigo'
        ,'CPF'
        ,'DataNascimento'
        ,'Nacionalidade'
        ,'Dependentes'
        ,'EstadoCivil'
        ,'Sexo'
        ,'Naturalidade'
        ,'Email'
        ,'Endereco'
        ,'Bairro'
        ,'Cidade'
        ,'Estado'
        ,'CEP'
        ,'TipoResidencia'
        ,'Telefone1'
        ,'TipoTelefone1'
        ,'Telefone2'
        ,'TipoTelefone2'
        )\
    .withColumnRenamed("PrpPFCodigo", "COD_PRP_PF")\
    .withColumnRenamed("CPF", "NUM_CPF")\
    .withColumnRenamed("DataNascimento", "DAT_NASC")\
    .withColumnRenamed("Nacionalidade", "DSC_NAC")\
    .withColumnRenamed("Dependentes", "NUM_DEP")\
    .withColumnRenamed("EstadoCivil", "DSC_EST_CIV")\
    .withColumnRenamed("Sexo", "DSC_SEX")\
    .withColumnRenamed("Naturalidade", "DSC_NAT")\
    .withColumnRenamed("Email", "DSC_EML")\
    .withColumnRenamed("Endereco", "DSC_END")\
    .withColumnRenamed("Bairro", "DSC_BRRO")\
    .withColumnRenamed("Cidade", "DSC_CID")\
    .withColumnRenamed("Estado", "DSC_EST")\
    .withColumnRenamed("CEP", "NUM_CEP")\
    .withColumnRenamed("TipoResidencia", "DSC_TIP_RES")\
    .withColumnRenamed("Telefone1", "NUM_TEL01")\
    .withColumnRenamed("TipoTelefone1", "TIP_TEL01")\
    .withColumnRenamed("Telefone2", "NUM_TEL02")\
    .withColumnRenamed("TipoTelefone2", "TIP_TEL02")\
    .withColumnRenamed("data_insercao", "DATA_INSCE")\
    .withColumnRenamed("data_atualizacao", "DATA_ATLC")

# COMMAND ----------

# VALIDA CNPJ
df_Propostaspf = df_Propostaspf.withColumn("FLG_CPF_VAL", identificar_campo_udf(df_Propostaspf["NUM_CPF"]))
df_Propostaspf = df_Propostaspf.withColumn("NUM_CPF_LIMPO",regexp_replace(col("NUM_CPF").cast("string"), "[^0-9]", ""))
df_Propostaspf = df_Propostaspf.withColumn("CEP_Limpo", regexp_replace(col("NUM_CEP"), "[^\\d]", ""))

# COMMAND ----------

df_Propostaspf.createOrReplaceGlobalTempView("propostaspf_temp_view")

result_status = dbutils.notebook.run(
    "/Users/higor.borges@upbrasil.com/01 - Bronze/Funcoes/Busca_Endereco",
    timeout_seconds=600,
    arguments={"temp_view_name": "propostaspf_temp_view"}
)

# COMMAND ----------

df_final = spark.sql("""
                    SELECT
                        COD_PRP_PF
                        ,NUM_CPF
                        ,DAT_NASC
                        ,DSC_NAC
                        ,NUM_DEP
                        ,DSC_EST_CIV
                        ,DSC_SEX
                        ,DSC_NAT
                        ,DSC_EML
                        ,DSC_TIP_RES
                        ,NUM_TEL01
                        ,TIP_TEL01
                        ,NUM_TEL02
                        ,TIP_TEL02
                        ,NUM_CPF_LIMPO
                        ,NUM_CEP
                        ,CEP_Limpo
                        ,DSC_END
                        ,DSC_BRRO
                        ,DSC_CID
                        ,DSC_EST
                        ,LOG_NO_LOG AS NME_LOG_VAL
                        ,BAI_NO_BAI AS NME_BAI_VAL
                        ,LOC_NO_LOC AS NME_CID_VAL
                        ,LOG_COMPLEMENTO_LOG AS LOG_COPTO_LOG_VAL
                        ,LOG_NO_ABREV_log AS NME_ABV_LOG_VAL
                        ,UFE_SG_LOG AS NME_UF_VAL
                        ,CEP_LOG AS CEP_VAL
                        ,FLG_CPF_VAL
                    from global_temp.propostaspf_temp_view_corrigido
""")

# COMMAND ----------

# Carregar a tabela Delta de destino como DeltaTable pelo nome do catálogo
deltaTable = DeltaTable.forName(spark, "bronzedev.default.propostaspf")

# Realizar o MERGE
(deltaTable.alias("dest")
 .merge(
     df_final.alias("src"),
     "dest.COD_PRP_PF = src.COD_PRP_PF"
 )
 .whenMatchedUpdateAll()   # Atualiza todas as colunas correspondentes
 .whenNotMatchedInsertAll() # Insere novas linhas que não correspondem
 .execute()
)

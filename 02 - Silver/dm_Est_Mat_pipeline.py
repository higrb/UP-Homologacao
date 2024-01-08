# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import expr, regexp_replace, row_number, lit
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Truncate da dimensão dm_Est_Mat
truncate_EstabelecimentoMatriz_df = spark.sql("truncate table silverdev.default.dm_est_mat")

# COMMAND ----------

# DBTITLE 1,Insert da dimensão dm_Est_Mat
insert_EstabelecimentoMatriz_df = spark.sql("select COD_ESTAB as COD_ESTAB, ifnull(upper(trim(NME_ESTAB)), 'N/I') as NME_ESTAB, ifnull(upper(trim(NME_RAZ_SOC)), 'N/I') as NME_RAZ_SOC, ifnull(upper(trim(NUM_CNPJ)), 'N/I') as NUM_CNPJ, ifnull(upper(trim(NUM_CNPJ_MTRZ)), 'N/I') as NUM_CNPJ_MTRZ, ifnull(upper(trim(NME_CID)), 'N/I') as NME_CID, ifnull(upper(trim(NME_EST)), 'N/I') as NME_EST, ifnull(upper(trim(NME_END)), 'N/I') as NME_END, ifnull(upper(trim(NME_BRRO)), 'N/I') as NME_BRRO, ifnull(upper(trim(NUM_CEP)), 'N/I') as NUM_CEP, ifnull(upper(trim(NUM_TEL)), 'N/I') as NUM_TEL, ifnull(upper(trim(DSC_EMAIL)), 'N/I') as DSC_EMAIL, ifnull(upper(trim(NME_SOC_PRP)), 'N/I') as NME_SOC_PRP, ifnull(upper(trim(NUM_CPF_SOC_PRP)), 'N/I') as NUM_CPF_SOC_PRP, ifnull(upper(trim(NUM_AGC)), 'N/I') as NUM_AGC, ifnull(upper(trim(NUM_CTA_CORR)), 'N/I') as NUM_CNT_CORR, cast(ifnull(DAT_CAD_ESTAB, '1900-01-01') as date) as DAT_CAD_ESTAB, cast(ifnull(DAT_VAL_CNPJ, '1900-01-01') as date) as DAT_VAL_CNPJ from bronzedev.default.estabelecimentos where NUM_CNPJ = NUM_CNPJ_MTRZ and COD_ESTAB in (select max(COD_ESTAB) as COD_ESTAB from bronzedev.default.estabelecimentos group by NUM_CNPJ)")

# COMMAND ----------

# DBTITLE 1,Retira caracteres
insert_EstabelecimentoMatriz_df = insert_EstabelecimentoMatriz_df.withColumn("NUM_CNPJ", regexp_replace("NUM_CNPJ", "[./-]", ""))
insert_EstabelecimentoMatriz_df = insert_EstabelecimentoMatriz_df.withColumn("NUM_CNPJ_MTRZ", regexp_replace("NUM_CNPJ_MTRZ", "[./-]", ""))
insert_EstabelecimentoMatriz_df = insert_EstabelecimentoMatriz_df.withColumn("NUM_CPF_SOC_PRP", regexp_replace("NUM_CPF_SOC_PRP", "[./-]", ""))
insert_EstabelecimentoMatriz_df = insert_EstabelecimentoMatriz_df.withColumn("NUM_TEL", regexp_replace("NUM_TEL", "[./-]", ""))
insert_EstabelecimentoMatriz_df = insert_EstabelecimentoMatriz_df.withColumn("NUM_CEP", regexp_replace("NUM_CEP", "[./-]", ""))

# COMMAND ----------

# DBTITLE 1,Add coluna PK
x = Window().orderBy(lit('A'))
insert_EstabelecimentoMatriz_df = insert_EstabelecimentoMatriz_df.withColumn("SRK_ESTAB_FILIAL", row_number().over(x))

# COMMAND ----------

# DBTITLE 1,Escreve na camada Silver
insert_EstabelecimentoMatriz_df.write.insertInto("silverdev.default.dm_Est_Mat", overwrite=True)

# COMMAND ----------

# DBTITLE 1,Insere registro não informado
nf_EstabelecimentoMatriz_df = spark.sql("select -1 as COD_ESTAB, 'N/I' as NME_ESTAB, 'N/I' as NME_RAZ_SOC, 'N/I' as NUM_CNPJ, 'N/I' as NUM_CNPJ_MTRZ, 'N/I' as NME_CIDADE, 'N/I' as NME_EST,  'N/I' as NME_END, 'N/I' as NME_BRRO, 'N/I' as NUM_CEP, 'N/I' as NUM_TEL, 'N/I' as DSC_EMAIL, 'N/I' as NME_SOC_PRP,'N/I' as NUM_CPF_SOC_PRP, 'N/I' as NUM_AGC, 'N/I' as NUM_CNT_CORR, cast('1900-01-01' as date) as DAT_CAD_ESTAB, cast('1900-01-01' as date) as DAT_VAL_CNPJ, -1 as SRK_ESTAB_FILIAL")

# COMMAND ----------

# DBTITLE 1,Escreve na camada Silver
nf_EstabelecimentoMatriz_df.write.insertInto("silverdev.default.dm_Est_Mat", overwrite=False)

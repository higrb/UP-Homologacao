# Databricks notebook source
# MAGIC %md
# MAGIC # Tabela Transação

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import expr, current_timestamp

# COMMAND ----------

# Foi adicionado uma etapa de transformação para selecionar um subconjunto de colunas na tabela landingzonedadosdev.default.transacoes e renomeá-las para nomes mais legíveis e intuitivos, respeitando os mnemonicos conforme documentação.
df_transacao = spark.table('landingzonedadosdev.default.transacoes')\
    .select(
        'TrnCodigo'
        ,'EstCodigo'
        ,'TpoTrnCodigo'
        ,'CrtUsrCodigo'
        ,'PagamentoMinimo'
        ,'MeiCptCodigo'
        ,'RdeCodigo'
        ,'Valor'
        ,'Data'
        ,'Parcelas'
        ,'DataGMT'
        ,'TaxaJuros'
        ,'DataAutorizacao'
        ,'DataSolicitacao'
        ,'Lote'
        ,'Comprovante'
        ,'Autorizacao'
        ,'Estorno'
        ,'TpoPrdCodigo'
        ,'Comissao'
        ,'PrdCodigo'
        ,'Distribuido'
        ,'CodEstab'
        ,'CodCliente'
        ,'CartaoEmv'
        ,'FallBack'
        ,'ModoEntrada'
        ,'Provedor'
        ,'Status'
        ,'TipoMensagem'
        ,'Terminal'
        ,'NSUOrigem'
        ,'DataLocal'
        ,'HoraLocal'
        ,'Comprovante_FormGen'
        ,'FlagTarifacao'
        ,'VinculoTransacao'
        ,'ReferenciaTecban'
        ,'TrnContestada'
        ,'DataRepasse'
        ,'EstabMigrado'
        ,'TrnReferencia'
        ,'SenhaCapturada'
        ,'UUID'
        ,'data_insercao'
        ,'data_atualizacao'
    )\
    .withColumnRenamed("TrnCodigo", "COD_TRN")\
    .withColumnRenamed("EstCodigo", "COD_ESTAB")\
    .withColumnRenamed("TpoTrnCodigo", "COD_TIP_TRN")\
    .withColumnRenamed("CrtUsrCodigo", "COD_CRT_USR")\
    .withColumnRenamed("PagamentoMinimo", "VAL_PGTO_MIN")\
    .withColumnRenamed("MeiCptCodigo", "COD_MEI_CPT")\
    .withColumnRenamed("RdeCodigo", "COD_RDE")\
    .withColumnRenamed("Valor", "VAL_TRN")\
    .withColumnRenamed("Data", "DAT_MOV_AUT")\
    .withColumnRenamed("Parcelas", "NUM_PARC")\
    .withColumnRenamed("DataGMT", "DAT_GMT")\
    .withColumnRenamed("TaxaJuros", "VAL_TXA_JUR")\
    .withColumnRenamed("DataAutorizacao", "DAT_AUT")\
    .withColumnRenamed("DataSolicitacao", "DAT_SOL")\
    .withColumnRenamed("Lote", "NUM_LOT")\
    .withColumnRenamed("Comprovante", "VAL_COMP")\
    .withColumnRenamed("Autorizacao", "NUM_AUT")\
    .withColumnRenamed("Estorno", "COD_EST")\
    .withColumnRenamed("TpoPrdCodigo", "COD_TIP_PRD")\
    .withColumnRenamed("Comissao", "VAL_CMSS")\
    .withColumnRenamed("PrdCodigo", "COD_PRD")\
    .withColumnRenamed("Distribuido", "COD_DISTR")\
    .withColumnRenamed("CodEstab", "COD_EMP")\
    .withColumnRenamed("CodCliente", "COD_ENT")\
    .withColumnRenamed("CartaoEmv", "COD_CTO_EMV")\
    .withColumnRenamed("FallBack", "FLG_FBCK")\
    .withColumnRenamed("ModoEntrada", "COD_MOD_ENT")\
    .withColumnRenamed("Provedor", "COD_PROV")\
    .withColumnRenamed("Status", "COD_STA_CTR")\
    .withColumnRenamed("TipoMensagem", "COD_TIP_MSG")\
    .withColumnRenamed("Terminal", "NUM_TERM")\
    .withColumnRenamed("NSUOrigem", "NSU_ORIG")\
    .withColumnRenamed("DataLocal", "DAT_LOC")\
    .withColumnRenamed("HoraLocal", "NUM_HR_LOC")\
    .withColumnRenamed("Comprovante_FormGen", "VAL_COMP_FORM_GEN")\
    .withColumnRenamed("FlagTarifacao", "FLG_TAR")\
    .withColumnRenamed("VinculoTransacao", "COD_VINC_TRN")\
    .withColumnRenamed("ReferenciaTecban", "COD_TCBN_REF")\
    .withColumnRenamed("TrnContestada", "COD_TRN_CONTEST")\
    .withColumnRenamed("DataRepasse", "DAT_REP")\
    .withColumnRenamed("EstabMigrado", "COD_ESTAB_MIGR")\
    .withColumnRenamed("TrnReferencia", "NUM_REF_TRN")\
    .withColumnRenamed("SenhaCapturada", "NUM_SNH_CAP")\
    .withColumnRenamed("UUID", "ID_TRN")\
    .withColumnRenamed("data_insercao", "DATA_INSCE")\
    .withColumnRenamed("data_atualizacao", "DATA_ATLC")

# COMMAND ----------

# DBTITLE 1,Tabela CartoesUsuarios
df_CartoesUsuarios = spark.table('bronzedev.default.CartoesUsuarios')\
    .select(
        'COD_CRT_USR',
        'COD_CTA_USR' 
        )


# COMMAND ----------

# DBTITLE 1,Tabela ContasUsuarios
df_ContasUsuarios = spark.table('bronzedev.default.ContasUsuarios')\
    .select(
        'COD_CNT_USR',
        'COD_PRP' 
        )

# COMMAND ----------

# DBTITLE 1,Tabela propostaspf
df_propostaspf = spark.table('bronzedev.default.propostaspf')\
    .select(
        'COD_PRP_PF',
        'NUM_CPF_LIMPO'        
        )

# COMMAND ----------

# DBTITLE 1,Tabela estabelecimentos
df_estabelecimentos = spark.table('bronzedev.default.estabelecimentos')\
    .select(
        'COD_ESTAB',
        'NUM_CNPJ_LIMPO',
        'NUM_CNPJ_LIMPO_MAT'
        )

# COMMAND ----------

# DBTITLE 1,Tabela entidades
df_entidades = spark.table('bronzedev.default.entidades')\
    .select(
        'COD_ENT',
        'NUM_CNPJ_LIMPO'
        )

# COMMAND ----------

df_transacao = df_transacao.join(df_CartoesUsuarios, "COD_CRT_USR", "left_outer")
df_transacao = df_transacao.join(df_ContasUsuarios, df_ContasUsuarios.COD_CNT_USR == df_transacao.COD_CTA_USR, "left_outer").drop(df_ContasUsuarios.COD_CNT_USR)
df_transacao = df_transacao.join(df_propostaspf, df_propostaspf.COD_PRP_PF == df_transacao.COD_PRP, "left_outer").drop(df_transacao.COD_PRP)
df_transacao = df_transacao.withColumnRenamed("NUM_CPF_LIMPO", "NUM_CPF_USU")
df_transacao = df_transacao.join(df_estabelecimentos, "COD_ESTAB", "left_outer")
df_transacao = df_transacao.withColumnRenamed("NUM_CNPJ_LIMPO", "NUM_CNPJ_ESTAB")
df_transacao = df_transacao.withColumnRenamed("NUM_CNPJ_LIMPO_MAT", "NUM_CNPJ_ESTAB_MTRZ")
df_transacao = df_transacao.join(df_entidades, "COD_ENT", "left_outer")
df_transacao = df_transacao.withColumnRenamed("NUM_CNPJ_LIMPO", "NUM_CNPJ_ENT")
df_transacao = df_transacao.dropDuplicates()

# COMMAND ----------

# Carregar a tabela Delta de destino como DeltaTable pelo nome do catálogo
deltaTable = DeltaTable.forName(spark, "bronzedev.default.transacao")

# Realizar o MERGE
(deltaTable.alias("dest")
 .merge(
     df_transacao.alias("src"),
     "dest.COD_TRN = src.COD_TRN AND dest.DAT_MOV_AUT = src.DAT_MOV_AUT"
 )
 .whenMatchedUpdateAll()   # Atualiza todas as colunas correspondentes
 .whenNotMatchedInsertAll() # Insere novas linhas que não correspondem
 .execute()
)

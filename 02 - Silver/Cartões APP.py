# Databricks notebook source
# MAGIC %md
# MAGIC ## Conexões - Banco de dados

# COMMAND ----------

url_processadora =  f"jdbc:sqlserver://{dbutils.secrets.get(scope='secretscopedev', key='host')};" + \
            f"databaseName={dbutils.secrets.get(scope='secretscopedev', key='dbprocessadora')};" + \
            "integratedSecurity=false;trustServerCertificate=true;"
url_policardnet =  f"jdbc:sqlserver://{dbutils.secrets.get(scope='secretscopedev', key='host')};" + \
            f"databaseName={dbutils.secrets.get(scope='secretscopedev', key='dbpolicard603078')};" + \
            "integratedSecurity=false;trustServerCertificate=true;"
url_usuarios =  f"jdbc:sqlserver://{dbutils.secrets.get(scope='secretscopedev', key='host')};" + \
            f"databaseName={dbutils.secrets.get(scope='secretscopedev', key='dbusuarios')};" + \
            "integratedSecurity=false;trustServerCertificate=true;"
url_sentinela =  f"jdbc:sqlserver://{dbutils.secrets.get(scope='secretscopedev', key='host')};" + \
            f"databaseName={dbutils.secrets.get(scope='secretscopedev', key='dbsentinela')};" + \
            "integratedSecurity=false;trustServerCertificate=true;"
properties = {
    "user": dbutils.secrets.get(scope="secretscopedev", key="user"),
    "password": dbutils.secrets.get(scope="secretscopedev", key="senhauser"),
    "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver"}

# COMMAND ----------

# MAGIC %md
# MAGIC ##Importação das tabelas

# COMMAND ----------

df_cnt_usu = spark.read.jdbc(
    url_processadora, "ContasUsuarios", properties=properties
).createOrReplaceTempView("ContasUsuariosProcessadora")
df_prp_usu = spark.read.jdbc(
    url_processadora, "Propostas", properties=properties
).createOrReplaceTempView("Propostas")
df_prppf_usu = spark.read.jdbc(
    url_processadora, "PropostasPF", properties=properties
).createOrReplaceTempView("PropostasPF")
df_crt_usr = spark.read.jdbc(
    url_processadora, "CartoesUsuarios", properties=properties
).createOrReplaceTempView("CartoesUsuarios")
df_entidades = spark.read.jdbc(
    url_processadora, "Entidades", properties=properties
).createOrReplaceTempView("Entidades")
df_estabelecimentos = spark.read.jdbc(
    url_processadora, "Estabelecimentos", properties=properties
).createOrReplaceTempView("Estabelecimentos")
df_tpo_prd = spark.read.jdbc(
    url_processadora, "TiposProdutos", properties=properties
).createOrReplaceTempView("TiposProdutos")
df_tpo_prd = spark.read.jdbc(
    url_processadora, "Transacoes", properties=properties
).createOrReplaceTempView("TransacoesProcessadora")
df_usu_pnet = spark.read.jdbc(
    url_policardnet, "Usuario", properties=properties
).createOrReplaceTempView("UsuarioPolicard")
df_crt_usu_pnet = spark.read.jdbc(
    url_policardnet, "Cartao_Usuario", properties=properties
).createOrReplaceTempView("Cartao_UsuarioPolicard")
df_cli_pnet = spark.read.jdbc(
    url_policardnet, "Cliente", properties=properties
).createOrReplaceTempView("ClientePolicard")
df_fornecedor = spark.read.jdbc(
    url_policardnet, "Fornecedor_CidadeUF", properties=properties
).createOrReplaceTempView("Fornecedor_CidadeUF")
df_tp_prd_pnet = spark.read.jdbc(
    url_policardnet, "TipoProduto", properties=properties
).createOrReplaceTempView("TipoProdutoPolicard")
df_trn_pnet = spark.read.jdbc(
    url_policardnet, "transacao_eletronica", properties=properties
).createOrReplaceTempView("transacao_eletronicaPolicard")
df_finan_pnet = spark.read.jdbc(
    url_policardnet, "Financiamento", properties=properties
).createOrReplaceTempView("Financiamento")
df_usu_ctr = spark.read.jdbc(
    url_usuarios, "UsuariosControles", properties=properties
).createOrReplaceTempView("UsuariosControles")
db_log_acesso = spark.read.jdbc(
    url_sentinela, "LOG_ACESSO_SISTEMAS", properties=properties
).createOrReplaceTempView("LOG_ACESSO_SISTEMAS")

# COMMAND ----------

# MAGIC %md
# MAGIC #Split campo auxiliar

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM landingzonedadosdev.default.usuarioscontrolessistemas
# MAGIC WHERE Status = 'A'

# COMMAND ----------

from pyspark.sql.functions import explode, split, col, regexp_extract

split_aux_df = _sqldf.withColumn(
    "Auxiliar", explode(split(col("Auxiliar"), ";"))
)
split_data = split(split_aux_df["Auxiliar"], ",")
columns = ["CodConta", "Franquia", "Cliente", "Produto", "codasa", "Ult4Digitos"]
for i, col_name in enumerate(columns):
    split_aux_df = split_aux_df.withColumn(col_name, split_data.getItem(i))
final_df = split_aux_df.select(["UsrCntCodigo", "Status"] + columns)
final_df.createOrReplaceTempView("CartoesApp")
display(final_df.show())

# COMMAND ----------

# MAGIC %md
# MAGIC #Tabelas de transação e log do sentinela

# COMMAND ----------

df_transacoes = spark.sql("""
SELECT 
    t.Codigo AS COD_TRN,
    t.Data AS DT_TRN,
    t.Valor_operacao AS VLR_TRN,
    t.Franquia_Usuario AS FRQ,
    t.Cartao_usuario AS CRT_USR_COD,
    'ACWEB' AS BASE,
    fc.Cidade AS CID_EC
FROM 
    transacao_eletronicaPolicard t
LEFT JOIN Fornecedor_CidadeUF fc ON t.Fornecedor = fc.Codigo AND t.Franquia_Fornecedor = fc.Franquia
WHERE T.Data BETWEEN date_trunc('month',date_add(current_date(),-90)) AND current_date()

UNION ALL

SELECT 
    f.Codigo AS COD_TRN,
    f.Data AS DT_TRN,
    f.Valor_base AS VLR_TRN,
    f.Franquia_Usuario AS FRQ,
    f.Cartao_usuario AS CRT_USR_COD,
    'ACWEB' AS BASE,
    e.Cidade AS CID_EC
FROM 
    Financiamento f
LEFT JOIN Fornecedor_CidadeUF fc ON f.Fornecedor = fc.Codigo AND f.Franquia_Fornecedor = fc.Franquia
LEFT JOIN Estabelecimentos e ON fc.Codigo_Novo = e.Numero
WHERE f.Data BETWEEN date_trunc('month',date_add(current_date(),-90)) AND current_date()

UNION ALL

SELECT 
    t.TrnCodigo AS COD_TRN,
    t.DATA AS DT_TRN,
    t.Valor AS VLR_TRN,
    0 AS FRQ,
    t.CrtUsrCodigo AS CRT_USR_COD,
    'SGP' AS BASE,
    e.Cidade AS CID_EC
FROM 
    TransacoesProcessadora t
LEFT JOIN Estabelecimentos e ON t.EstCodigo = e.EstCodigo 
WHERE t.Data BETWEEN date_trunc('month',date_add(current_date(),-90)) AND current_date()
""")

# COMMAND ----------

df_sentinela = spark.sql("""
       SELECT l.codigo AS COD_LOG_APP,
              l.usuario AS CNT_USR_COD,
              l.franquia AS FRQ,
              l.cliente AS ENT_COD,
              l.data as DT_LOG_APP
       FROM LOG_ACESSO_SISTEMAS l
       WHERE l.Sistema = 57
       AND l.data BETWEEN date_trunc('month',date_add(current_date(),-90)) AND current_date()
       ORDER BY Data"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Dimensão dos cartões no app

# COMMAND ----------

#BASE 1 ACWEB, BASE 2 SGP

df_consolidado = spark.sql("""SELECT
        ca.UsrCntCodigo AS USR_CNT_COD,
        ca.CodConta AS CNT_USR_COD,
        '0' AS PRP_COD,
        ca.Franquia AS FRQ,
        ca.Cliente AS ENT_COD,
        cu.Codigo AS CRT_USR_COD,
        ca.Produto AS TPO_PRD_COD,
        tp.Nome AS NOM_PRD,
        ca.CodASA AS TIP_PROD,
        cu.Status as STAT_CRT,  
        0 as CRED_DISP,
        cu.Chip AS CHIP,
        'ACWEB' AS BASE,
        up.CPF AS CPF,
        uc.Telefone AS TEL,
        CAST(up.Bairro AS VARCHAR(100)) AS BAIRRO,
        CAST(up.Cidade AS VARCHAR(100)) AS CID,
        CAST(cli.Cidade AS VARCHAR(100)) AS CID_CLI,
        up.UF AS EST,
        up.CEP AS CEP
    FROM CartoesApp ca
    JOIN UsuarioPolicard up ON ca.CodConta = up.Codigo and ca.Franquia = up.Franquia
    JOIN UsuariosControles uc ON ca.UsrCntCodigo = uc.UsrCntCodigo
    JOIN Cartao_UsuarioPolicard cu ON ca.CodConta = cu.Usuario and cu.Franquia = ca.franquia and ca.Ult4Digitos = RIGHT(cu.CodigoCartao,4)
    JOIN ClientePolicard cli ON ca.Cliente = cli.Codigo and ca.Franquia = cli.Franquia
    JOIN TiposProdutos TP on TP.TpoPrdCodigo = ca.Produto
    WHERE ca.CodASA != 0 AND cu.Status in ('A','B') AND up.Status = 'A'

    UNION ALL

    SELECT
        ca.UsrCntCodigo AS USR_CNT_COD,
        ca.CodConta AS CNT_USR_COD,
        cup.PrpCodigo AS PRP_COD,
        0 AS FRQ,
        ca.Cliente AS ENT_COD,
        cu.CrtUsrCodigo AS CRT_USR_COD,
        ca.Produto AS TPO_PRD_COD,
        tp.Nome AS NOM_PRD,
        ca.CodASA AS TIP_PROD,
        cu.Status AS STAT_CRT,  
        CAST(cu.CreditoDisponivel AS VARCHAR(100)) AS CRED_DISP,
        cu.Chip AS CHIP,
        'SGP' AS BASE,
        pF.CPF AS CPF,
        uc.Telefone AS TEL,
        CAST(pf.Bairro AS VARCHAR(100)) AS BAIRRO,
        CAST(pf.Cidade AS VARCHAR(100)) AS CID,
        CAST(e.Cidade AS VARCHAR(100)) AS CID_CLI,
        pf.Estado AS EST,
        pf.CEP AS CEP
    FROM CartoesApp ca
    JOIN ContasUsuariosProcessadora cup ON ca.CodConta = cup.CntUsrCodigo 
    JOIN Propostas p ON cup.PrpCodigo = p.PrpCodigo
    JOIN PropostasPF pf ON p.PrpCodigo = pf.PrpPFCodigo
    JOIN UsuariosControles uc ON ca.UsrCntCodigo = uc.UsrCntCodigo
    JOIN CartoesUsuarios cu ON ca.CodConta = cu.CntUsrCodigo and ca.Ult4Digitos = RIGHT(cu.Numero,4)
    JOIN TiposProdutos tp ON ca.Produto = tp.TpoPrdCodigo 
    JOIN Entidades E ON ca.Cliente = E.EntCodigo
    WHERE ca.CodASA = 0 AND cu.Status in ('A','B') AND cup.Status = 'A'
    """)

# COMMAND ----------

# df_consolidado.createOrReplaceTempView("df_consolidado")
# df_sentinela.createOrReplaceTempView("df_sentinela")
# df_teste = spark.sql("""
#             SELECT MONTH(s.DT_LOG_APP) as Data,
#                     COUNT(DISTINCT c.USR_CNT_COD) as Count
#             FROM df_consolidado c
#             JOIN df_sentinela s ON c.CNT_USR_COD = s.CNT_USR_COD AND c.ENT_COD = s.ENT_COD
#             GROUP BY MONTH(s.DT_LOG_APP)
# """)
# df_teste.show(5)

# COMMAND ----------

df_consolidado.write.mode("overwrite").format("delta").saveAsTable("golddev.default.app")

# COMMAND ----------

df_sentinela.write.mode("overwrite").format("delta").saveAsTable("golddev.default.sentinelaapp")

# COMMAND ----------

df_transacoes.write.mode("overwrite").format("delta").saveAsTable("golddev.default.transacoesapp")

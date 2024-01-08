# Databricks notebook source
# MAGIC %md
# MAGIC #Dados Processadora
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

url =  f"jdbc:sqlserver://{dbutils.secrets.get(scope='secretscopedev', key='host')};" + \
            f"databaseName={dbutils.secrets.get(scope='secretscopedev', key='dbprocessadora')};" + \
            "integratedSecurity=false;trustServerCertificate=true;"

properties = {
    "user": dbutils.secrets.get(scope="secretscopedev", key="user"),
    "password": dbutils.secrets.get(scope="secretscopedev", key="senhauser"),
    "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

dataframes  = {}

lista=['transacoes'
,'Estabelecimentos'
,'CartoesUsuarios'
,'ContasUsuarios'
,'Propostas'
,'Entidades'
,'TiposProdutos'
,'PropostasPF'
,'ProdutosAgentesEmissores'
,'ContasCorrentesUsuarios'
,'Pedidoslancamentosfinanceiro'
,'TransacoesNegadas'
,'TransacoesDesfeitas']



# COMMAND ----------

for item in lista:
    dataframes[item] = spark.read \
        .jdbc(url, item, properties=properties)

    dataframes[item].createOrReplaceTempView(item)

# COMMAND ----------

# MAGIC %md
# MAGIC # Transacoes SGP
# MAGIC  Os comando abaixo tem como finalidade de substituir todas as tabelas abaixo com dados d-1:
# MAGIC  - transacoes
# MAGIC  - Estabelecimentos
# MAGIC  - CartoesUsuarios
# MAGIC  - ContasUsuarios
# MAGIC  - Propostas
# MAGIC  - Entidades
# MAGIC  - TiposProdutos
# MAGIC  - Propostas PF
# MAGIC  - ProdutosAgentesEmissores
# MAGIC  - ContasCorrentesUsuarios
# MAGIC  - Pedidoslancamentosfinanceiro
# MAGIC  - TransacoesNegadas
# MAGIC  - TransacoesDesfeitas
# MAGIC
# MAGIC Nesta etapa, tratamos os dados como uma stage.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Criação Tabela transacoes

# COMMAND ----------

# DBTITLE 0,Insert transacoes
# MAGIC %sql
# MAGIC /*CREATE OR REPLACE TABLE landingzonedadosdev.default.transacoes
# MAGIC using delta(
# MAGIC SELECT
# MAGIC   T.*
# MAGIC   ,current_timestamp() as data_insercao
# MAGIC   ,current_timestamp() as data_atualizacao
# MAGIC FROM transacoes t
# MAGIC WHERE
# MAGIC   date_format(t.data, 'yyyy-MM-dd') >= date_format(dateadd(day, -730, current_date()), 'yyyy-MM-dd')
# MAGIC GROUP BY ALL
# MAGIC );*/

# COMMAND ----------

# MAGIC %md
# MAGIC ##Criação Tabela Estabelecimentos

# COMMAND ----------

# MAGIC %sql
# MAGIC /*CREATE OR REPLACE TABLE landingzonedadosdev.default.Estabelecimentos
# MAGIC using delta(
# MAGIC select
# MAGIC   e.*
# MAGIC   ,current_timestamp() as data_insercao
# MAGIC   ,current_timestamp() as data_atualizacao
# MAGIC from Estabelecimentos e
# MAGIC --where  date_format(e.data, 'yyyy-MM-dd') = date_format(dateadd(day, -730, current_date()), 'yyyy-MM-dd')
# MAGIC GROUP BY ALL
# MAGIC )*/

# COMMAND ----------

# MAGIC %md
# MAGIC ##Criação Tabela CartoesUsuarios

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE landingzonedadosdev.default.CartoesUsuarios
# MAGIC using delta(
# MAGIC select
# MAGIC   CU.*
# MAGIC   ,current_timestamp() as data_insercao
# MAGIC   ,current_timestamp() as data_atualizacao
# MAGIC from CartoesUsuarios cu
# MAGIC --where  date_format(cu.DataEmissao, 'yyyy-MM-dd') = date_format(dateadd(day, -1, current_date()), 'yyyy-MM-dd')
# MAGIC GROUP BY ALL
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Criação Tabela ContasUsuarios

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE landingzonedadosdev.default.ContasUsuarios
# MAGIC using delta(
# MAGIC select
# MAGIC   cnt.*
# MAGIC   ,current_timestamp() as data_insercao
# MAGIC   ,current_timestamp() as data_atualizacao
# MAGIC from ContasUsuarios cnt
# MAGIC --where date_format(cnt.DataCriacao, 'yyyy-MM-dd') = date_format(dateadd(day, -1, current_date()), 'yyyy-MM-dd')
# MAGIC GROUP BY ALL
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Criação Tabela Propostas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE landingzonedadosdev.default.Propostas
# MAGIC using delta(
# MAGIC select
# MAGIC   PRP.*
# MAGIC   ,current_timestamp() as data_insercao
# MAGIC   ,current_timestamp() as data_atualizacao
# MAGIC from Propostas prp
# MAGIC --where   date_format(prp.Data, 'yyyy-MM-dd') = date_format(dateadd(day, -1, current_date()), 'yyyy-MM-dd')
# MAGIC GROUP BY ALL
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Criação Tabela Propostas PF

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE landingzonedadosdev.default.PropostasPf
# MAGIC using delta(
# MAGIC select
# MAGIC   PF.*
# MAGIC   ,current_timestamp() as data_insercao
# MAGIC   ,current_timestamp() as data_atualizacao
# MAGIC from propostas prp
# MAGIC inner join Propostaspf pf on pf.PrpPFCodigo = prp.PrpCodigo
# MAGIC --where  date_format(prp.Data, 'yyyy-MM-dd') = date_format(dateadd(day, -1, current_date()), 'yyyy-MM-dd')
# MAGIC GROUP BY ALL
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Criação Tabela Entidades

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE landingzonedadosdev.default.Entidades
# MAGIC using delta(
# MAGIC select
# MAGIC   *
# MAGIC   ,current_timestamp() as data_insercao
# MAGIC   ,current_timestamp() as data_atualizacao
# MAGIC from Entidades
# MAGIC GROUP BY ALL
# MAGIC )

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

lista=['ContasEntidades'
,'TransacoesLancamentosContas'
,'Redes'
,'TiposTransacoes'
,'TiposLancamentos'
,'LancamentosFaturas'
,'LancamentosContas'
,'LancamentosInternosLancamentosFaturas'
,'LancamentosInternos'
,'EstabelecimentosTiposProdutos'
]

# COMMAND ----------

for item in lista:
    dataframes[item] = spark.read \
        .jdbc(url, item, properties=properties)

    dataframes[item].createOrReplaceTempView(item)

# COMMAND ----------

# MAGIC %md
# MAGIC # Parâmetros do SGP
# MAGIC  Os comando abaixo tem como finalidade de substituir todas as tabelas abaixo com dados d-1:
# MAGIC
# MAGIC  - ContasEntidades
# MAGIC  - TransacoesLancamentosContas
# MAGIC  - Redes
# MAGIC  - TiposTransacoes
# MAGIC  - TiposLancamentos
# MAGIC  - LancamentosFaturas
# MAGIC  - LancamentosInternosLancamentosFaturas
# MAGIC  - LancamentosInternos
# MAGIC  - EstabelecimentosTiposProdutos
# MAGIC
# MAGIC Nesta etapa, tratamos os dados como uma stage.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Criação Tabela ContasEntidades

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE landingzonedadosdev.default.ContasEntidades
# MAGIC using delta(
# MAGIC select
# MAGIC   *
# MAGIC   ,current_timestamp() as data_insercao
# MAGIC   ,current_timestamp() as data_atualizacao
# MAGIC from ContasEntidades
# MAGIC GROUP BY ALL
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Criação Tabela TransacoesLancamentosContas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE landingzonedadosdev.default.TransacoesLancamentosContas
# MAGIC using delta(
# MAGIC select 
# MAGIC tlc.* 
# MAGIC from LancamentosContas lc
# MAGIC INNER JOIN TransacoesLancamentosContas tlc ON lc.LncCntCodigo = tlc.LncCntCodigo
# MAGIC where
# MAGIC     date_format(lc.Data, 'yyyy-MM-dd') = date_format(dateadd(day, -1, current_date()), 'yyyy-MM-dd')
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Criação Tabela Redes

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE landingzonedadosdev.default.redes
# MAGIC using delta(
# MAGIC select
# MAGIC   *
# MAGIC   ,current_timestamp() as data_insercao
# MAGIC   ,current_timestamp() as data_atualizacao
# MAGIC from redes
# MAGIC GROUP BY ALL
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Criação Tabela TiposTransacoes

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE landingzonedadosdev.default.TiposTransacoes
# MAGIC using delta(
# MAGIC select
# MAGIC   *
# MAGIC   ,current_timestamp() as data_insercao
# MAGIC   ,current_timestamp() as data_atualizacao
# MAGIC from TiposTransacoes
# MAGIC GROUP BY ALL
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Criação Tabela TiposLancamentos

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE landingzonedadosdev.default.TiposLancamentos
# MAGIC using delta(
# MAGIC select
# MAGIC   *
# MAGIC   ,current_timestamp() as data_insercao
# MAGIC   ,current_timestamp() as data_atualizacao
# MAGIC from TiposLancamentos
# MAGIC GROUP BY ALL
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Criação Tabela LancamentosFaturas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE landingzonedadosdev.default.LancamentosFaturas
# MAGIC using delta(
# MAGIC select
# MAGIC   *
# MAGIC   ,current_timestamp() as data_insercao
# MAGIC   ,current_timestamp() as data_atualizacao
# MAGIC from LancamentosFaturas
# MAGIC GROUP BY ALL
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Criação Tabela LancamentosInternosLancamentosFaturas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE landingzonedadosdev.default.LancamentosInternosLancamentosFaturas
# MAGIC using delta(
# MAGIC select
# MAGIC   *
# MAGIC   ,current_timestamp() as data_insercao
# MAGIC   ,current_timestamp() as data_atualizacao
# MAGIC from LancamentosInternosLancamentosFaturas
# MAGIC GROUP BY ALL
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Criação Tabela LancamentosInternos

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE landingzonedadosdev.default.LancamentosInternos
# MAGIC using delta(
# MAGIC select
# MAGIC   *
# MAGIC   ,current_timestamp() as data_insercao
# MAGIC   ,current_timestamp() as data_atualizacao
# MAGIC from LancamentosInternos
# MAGIC GROUP BY ALL
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Criação Tabela EstabelecimentosTiposProdutos

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE landingzonedadosdev.default.EstabelecimentosTiposProdutos
# MAGIC using delta(
# MAGIC select
# MAGIC   *
# MAGIC   ,current_timestamp() as data_insercao
# MAGIC   ,current_timestamp() as data_atualizacao
# MAGIC from EstabelecimentosTiposProdutos
# MAGIC GROUP BY ALL
# MAGIC )

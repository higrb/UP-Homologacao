# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, sequence, to_date

spark = SparkSession.builder.appName("gerar_datas").getOrCreate()

data_inicio = "1990-01-01"
data_fim = "2050-12-31"

df_datas = spark.sql(f"""
    SELECT explode(sequence(to_date('{data_inicio}'), to_date('{data_fim}'), interval 1 day)) as data
""")

df_datas.createOrReplaceTempView("temp_dat")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE BRONZEDEV.DEFAULT.PERIODO
# MAGIC USING DELTA(
# MAGIC SELECT
# MAGIC     SUB1.DATA AS DAT,
# MAGIC     sub1.mes as MES ,
# MAGIC     SUB1.DIA as DIA,
# MAGIC     SUB1.numero_data AS NUM_DAT,
# MAGIC     SUB1.numero_ano AS NUM_ANO,
# MAGIC     SUB1.numero_mes AS NUM_MES,
# MAGIC     SUB1.numero_dia AS NUM_DIA,
# MAGIC     SUB1.desc_mes_ano AS DSC_MES_ANO,
# MAGIC     SUB1.desc_mes_ano_dia AS DSC_MES_ANO_DIA,
# MAGIC     SUB1.desc_abv_mes AS DSC_ABV_MES,
# MAGIC     SUB1.desc_abv_mes_dia AS DSC_ABV_MES_DIA,
# MAGIC     SUB1.desc_bim AS DSC_BIM,
# MAGIC     SUB1.desc_tri AS DSC_TRI,
# MAGIC     SUB1.desc_sem AS DSC_SEM,
# MAGIC     SUB1.desc_bim_ano AS DSC_BIM_ANO,
# MAGIC     SUB1.desc_tri_ano AS DSC_TRI_ANO,
# MAGIC     SUB1.desc_sem_ano AS DSC_SEM_ANO,
# MAGIC     SUB1.flg_prm_dia AS FLG_PRM_DIA,
# MAGIC     SUB1.flg_ult_dia AS FLG_ULT_DIA,
# MAGIC     CASE 
# MAGIC         WHEN (SUB1.DATA = FER.DATA AND dayofweek(SUB1.data) BETWEEN 2 AND 6) 
# MAGIC             THEN 0
# MAGIC             ELSE 1 
# MAGIC     END FLG_DIA_UTL,
# MAGIC     SUB1.qtd_dia_ano AS QTD_DIA_ANO,
# MAGIC     SUB1.qtd_sem_ano AS QTD_SEM_ANO,
# MAGIC     SUB1.qtd_dia_rst_ano AS QTD_DIA_RST_ANO
# MAGIC FROM
# MAGIC     (
# MAGIC     SELECT
# MAGIC         data,
# MAGIC         CASE 
# MAGIC             WHEN month(data) = 1 THEN 'Janeiro'
# MAGIC             WHEN month(data) = 2 THEN 'Fevereiro'
# MAGIC             WHEN month(data) = 3 THEN 'Março'
# MAGIC             WHEN month(data) = 4 THEN 'Abril'
# MAGIC             WHEN month(data) = 5 THEN 'Maio'
# MAGIC             WHEN month(data) = 6 THEN 'Junho'
# MAGIC             WHEN month(data) = 7 THEN 'Julho'
# MAGIC             WHEN month(data) = 8 THEN 'Agosto'
# MAGIC             WHEN month(data) = 9 THEN 'Setembro'
# MAGIC             WHEN month(data) = 10 THEN 'Outubro'
# MAGIC             WHEN month(data) = 11 THEN 'Novembro'
# MAGIC             WHEN month(data) = 12 THEN 'Dezembro'
# MAGIC         END as mes, -- Mês por extenso
# MAGIC         CASE 
# MAGIC             WHEN dayofweek(data) = 1 THEN 'Domingo'
# MAGIC             WHEN dayofweek(data) = 2 THEN 'Segunda-feira'
# MAGIC             WHEN dayofweek(data) = 3 THEN 'Terça-feira'
# MAGIC             WHEN dayofweek(data) = 4 THEN 'Quarta-feira'
# MAGIC             WHEN dayofweek(data) = 5 THEN 'Quinta-feira'
# MAGIC             WHEN dayofweek(data) = 6 THEN 'Sexta-feira'
# MAGIC             WHEN dayofweek(data) = 7 THEN 'Sábado'
# MAGIC         END as dia, -- Dia da semana por extenso
# MAGIC         date_format(data, 'yyyyMMdd') as numero_data,
# MAGIC         year(data) as numero_ano,
# MAGIC         month(data) as numero_mes,
# MAGIC         day(data) as numero_dia,
# MAGIC         CONCAT(CASE 
# MAGIC                 WHEN month(data) = 1 THEN 'Janeiro'
# MAGIC                 WHEN month(data) = 2 THEN 'Fevereiro'
# MAGIC                 WHEN month(data) = 3 THEN 'Março'
# MAGIC                 WHEN month(data) = 4 THEN 'Abril'
# MAGIC                 WHEN month(data) = 5 THEN 'Maio'
# MAGIC                 WHEN month(data) = 6 THEN 'Junho'
# MAGIC                 WHEN month(data) = 7 THEN 'Julho'
# MAGIC                 WHEN month(data) = 8 THEN 'Agosto'
# MAGIC                 WHEN month(data) = 9 THEN 'Setembro'
# MAGIC                 WHEN month(data) = 10 THEN 'Outubro'
# MAGIC                 WHEN month(data) = 11 THEN 'Novembro'
# MAGIC                 WHEN month(data) = 12 THEN 'Dezembro'
# MAGIC             END, '/', year(data)) as desc_mes_ano,
# MAGIC             CONCAT(year(data), '/', CASE 
# MAGIC                 WHEN month(data) = 1 THEN 'Janeiro'
# MAGIC                 WHEN month(data) = 2 THEN 'Fevereiro'
# MAGIC                 WHEN month(data) = 3 THEN 'Março'
# MAGIC                 WHEN month(data) = 4 THEN 'Abril'
# MAGIC                 WHEN month(data) = 5 THEN 'Maio'
# MAGIC                 WHEN month(data) = 6 THEN 'Junho'
# MAGIC                 WHEN month(data) = 7 THEN 'Julho'
# MAGIC                 WHEN month(data) = 8 THEN 'Agosto'
# MAGIC                 WHEN month(data) = 9 THEN 'Setembro'
# MAGIC                 WHEN month(data) = 10 THEN 'Outubro'
# MAGIC                 WHEN month(data) = 11 THEN 'Novembro'
# MAGIC                 WHEN month(data) = 12 THEN 'Dezembro'
# MAGIC             END, '/', day(data)) as desc_mes_ano_dia,
# MAGIC             CONCAT(right(year(data), 2), '/', SUBSTRING(CASE 
# MAGIC                 WHEN month(data) = 1 THEN 'Janeiro'
# MAGIC                 WHEN month(data) = 2 THEN 'Fevereiro'
# MAGIC                 WHEN month(data) = 3 THEN 'Março'
# MAGIC                 WHEN month(data) = 4 THEN 'Abril'
# MAGIC                 WHEN month(data) = 5 THEN 'Maio'
# MAGIC                 WHEN month(data) = 6 THEN 'Junho'
# MAGIC                 WHEN month(data) = 7 THEN 'Julho'
# MAGIC                 WHEN month(data) = 8 THEN 'Agosto'
# MAGIC                 WHEN month(data) = 9 THEN 'Setembro'
# MAGIC                 WHEN month(data) = 10 THEN 'Outubro'
# MAGIC                 WHEN month(data) = 11 THEN 'Novembro'
# MAGIC                 WHEN month(data) = 12 THEN 'Dezembro'
# MAGIC             END, 1, 3)) as desc_abv_mes,
# MAGIC             CONCAT(right(year(data), 2), '/', SUBSTRING(CASE 
# MAGIC                 WHEN month(data) = 1 THEN 'Janeiro'
# MAGIC                 WHEN month(data) = 2 THEN 'Fevereiro'
# MAGIC                 WHEN month(data) = 3 THEN 'Março'
# MAGIC                 WHEN month(data) = 4 THEN 'Abril'
# MAGIC                 WHEN month(data) = 5 THEN 'Maio'
# MAGIC                 WHEN month(data) = 6 THEN 'Junho'
# MAGIC                 WHEN month(data) = 7 THEN 'Julho'
# MAGIC                 WHEN month(data) = 8 THEN 'Agosto'
# MAGIC                 WHEN month(data) = 9 THEN 'Setembro'
# MAGIC                 WHEN month(data) = 10 THEN 'Outubro'
# MAGIC                 WHEN month(data) = 11 THEN 'Novembro'
# MAGIC                 WHEN month(data) = 12 THEN 'Dezembro'
# MAGIC             END, 1, 3), '/', day(data)) as desc_abv_mes_dia,
# MAGIC             CASE 
# MAGIC                 WHEN month(data) IN (1, 2) THEN '1º Bimestre'
# MAGIC                 WHEN month(data) IN (3, 4) THEN '2º Bimestre'
# MAGIC                 WHEN month(data) IN (5, 6) THEN '3º Bimestre'
# MAGIC                 WHEN month(data) IN (7, 8) THEN '4º Bimestre'
# MAGIC                 WHEN month(data) IN (9, 10) THEN '5º Bimestre'
# MAGIC                 WHEN month(data) IN (11, 12) THEN '6º Bimestre'
# MAGIC             END as desc_bim,
# MAGIC             CASE 
# MAGIC                 WHEN month(data) IN (1, 2, 3) THEN '1º Trimestre'
# MAGIC                 WHEN month(data) IN (4, 5, 6) THEN '2º Trimestre'
# MAGIC                 WHEN month(data) IN (7, 8, 9) THEN '3º Trimestre'
# MAGIC                 WHEN month(data) IN (10, 11, 12) THEN '4º Trimestre'
# MAGIC             END as desc_tri,
# MAGIC             CASE 
# MAGIC                 WHEN month(data) IN (1, 2, 3, 4, 5, 6) THEN '1º Semestre'
# MAGIC                 WHEN month(data) IN (7, 8, 9, 10, 11, 12) THEN '2º Semestre'
# MAGIC             END as desc_sem,
# MAGIC             CONCAT(
# MAGIC                     CASE 
# MAGIC                         WHEN month(data) IN (1, 2) THEN '1º Bimestre'
# MAGIC                         WHEN month(data) IN (3, 4) THEN '2º Bimestre'
# MAGIC                         WHEN month(data) IN (5, 6) THEN '3º Bimestre'
# MAGIC                         WHEN month(data) IN (7, 8) THEN '4º Bimestre'
# MAGIC                         WHEN month(data) IN (9, 10) THEN '5º Bimestre'
# MAGIC                         WHEN month(data) IN (11, 12) THEN '6º Bimestre'
# MAGIC                     END, 
# MAGIC                     ' ', year(data)
# MAGIC                 ) as desc_bim_ano,
# MAGIC                 CONCAT(
# MAGIC                     CASE 
# MAGIC                         WHEN month(data) IN (1, 2, 3) THEN '1º Trimestre'
# MAGIC                         WHEN month(data) IN (4, 5, 6) THEN '2º Trimestre'
# MAGIC                         WHEN month(data) IN (7, 8, 9) THEN '3º Trimestre'
# MAGIC                         WHEN month(data) IN (10, 11, 12) THEN '4º Trimestre'
# MAGIC                     END, 
# MAGIC                     ' ', year(data)
# MAGIC                 ) as desc_tri_ano,
# MAGIC                 CONCAT(
# MAGIC                     CASE 
# MAGIC                         WHEN month(data) IN (1, 2, 3, 4, 5, 6) THEN '1º Semestre'
# MAGIC                         WHEN month(data) IN (7, 8, 9, 10, 11, 12) THEN '2º Semestre'
# MAGIC                     END, 
# MAGIC                     ' ', year(data)
# MAGIC                 ) as desc_sem_ano,
# MAGIC         CASE WHEN dayofmonth(data) = 1 THEN 1 ELSE 0 END as flg_prm_dia,
# MAGIC         CASE WHEN day(data) = day(last_day(data)) THEN 1 ELSE 0 END as flg_ult_dia,
# MAGIC         dayofyear(data) as qtd_dia_ano,
# MAGIC         weekofyear(data) as qtd_sem_ano,
# MAGIC         (365 + CASE WHEN month(data) = 2 AND day(data) = 29 THEN 1 ELSE 0 END) - dayofyear(data) as qtd_dia_rst_ano
# MAGIC     FROM temp_dat
# MAGIC     )sub1 LEFT join landingzonedadosdev.correios.feriado fer on fer.data = sub1.data
# MAGIC )

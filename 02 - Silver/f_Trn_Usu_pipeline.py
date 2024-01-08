# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import expr, regexp_replace, row_number, lit
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Truncate da fato f_Trn_Usu
truncate_Transacao_Usuario_df = spark.sql("truncate table silverdev.default.f_Trn_Usu")

# COMMAND ----------

# DBTITLE 1,Insert da fato f_Trn_Usu
insert_Transacao_Usuario_df = spark.sql("select distinct t.COD_TRN, ifnull(pmov.SRK_DAT, -1) as SRK_DAT_MOV_AUT, ifnull(fi.SRK_ESTAB_FILIAL, -1) as SRK_ESTAB_FILIAL, ifnull(ma.SRK_ESTAB_mATRIZ, -1) as SRK_ESTAB_MATRIZ, ifnull(cli.SRK_CLI, -1) as SRK_CLI, ifnull(usu.COD_PRP_PF, -1) as SRK_USUARIO, ifnull(tipprd.SRK_TIP_PRD, -1) as SRK_TIP_PRD, ifnull(t.COD_TIP_TRN, -1) as COD_TIP_TRN, ifnull(t.COD_MEI_CPT, -1) as COD_MEI_CPT, ifnull(t.COD_RDE, -1) as COD_RDE, ifnull(t.COD_EST, 'N/I') as COD_EST, ifnull(t.COD_PRD, -1) as COD_PRD, ifnull(t.COD_DISTR, -1) as COD_DISTR, ifnull(t.COD_EMP, -1) as COD_EMP, ifnull(upper(trim(t.COD_CTO_EMV)), 'N/I') as COD_CTO_EMV, ifnull(upper(trim(t.COD_ESTAB_MIGR)), 'N/I') as COD_ESTAB_MIGR, ifnull(upper(trim(t.COD_MOD_ENT)), 'N/I') as COD_MOD_ENT, ifnull(upper(trim(t.COD_TIP_MSG)), 'N/I') as COD_TIP_MSG, ifnull(upper(trim(t.COD_PROV)), 'N/I') as COD_PROV, ifnull(upper(trim(t.COD_STA_CTR)), 'N/I') as COD_STA_CTR, ifnull(upper(trim(t.COD_TCBN_REF)), 'N/I') as COD_TCBN_REF, ifnull(upper(trim(t.COD_TRN_CONTEST)), 'N/I') as COD_TRN_CONTEST, ifnull(t.NUM_PARC, -1) as NUM_PARC, ifnull(t.NUM_AUT, -1) as NUM_AUT, ifnull(upper(trim(t.NUM_TERM)), 'N/I') as NUM_TERM, ifnull(t.NUM_HR_LOC, -1) as NUM_HR_LOC, ifnull(t.NUM_LOT, -1) as NUM_LOT, ifnull(t.NUM_REF_TRN, -1) as NUM_REF_TRN, ifnull(upper(trim(t.NUM_SENH_CAPTUR)), 'N/I') as NUM_SENH_CAPTUR, ifnull(t.DAT_LOC, -1) as DAT_LOC, ifnull(pgmt.SRK_DAT, -1) as SRK_DAT_GMT, ifnull(paut.SRK_DAT, -1) as SRK_DAT_AUT, ifnull(psol.SRK_DAT, -1) as SRK_DAT_SOL, ifnull(prepas.SRK_DAT, -1) as SRK_DAT_REPASS, ifnull(t.VAL_COMP, 0) as VAL_COMP, ifnull(t.VAL_TXA_JUR, 0) as VAL_TXA_JUR, ifnull(t.VAL_TRN, 0) as VAL_TRN, ifnull(t.VAL_COMP_FORM_GEN, 0) as VAL_COMP_FORM_GEN, ifnull(t.VAL_PGTO_MIN, 0) as VAL_PGTO_MIN, ifnull(t.VAL_CMSS, 0) as VAL_CMSS, ifnull(upper(trim(t.FLG_FBCK)), 'N/I') as FLG_FBCK, ifnull(upper(trim(t.NSU_ORIG)), 'N/I') as NSU_ORIG, ifnull(upper(trim(t.FLG_TAR)), 'N/I') as FLG_TAR, ifnull(upper(trim(t.ID_TRN)), 'N/I') as ID_TRN from bronzedev.default.transacao t left join silverdev.default.dm_Est_Fil fi on (fi.COD_ESTAB = t.COD_ESTAB) left join silverdev.default.dm_Est_Mat ma on (ma.COD_ESTAB = t.COD_ESTAB) left join silverdev.default.dm_cli cli on (cli.COD_ENT = t.COD_ENT) left join landingzonedadosdev.default.CartoesUsuarios crtusu on (crtusu.CrtUsrCodigo  = t.COD_CRT_USR) left join bronzedev.default.ContasUsuarios ctausu on (ctausu.COD_CNT_USR = crtusu.CntUsrCodigo) left join silverdev.default.dm_Usu usu on (usu.COD_PRP_PF = ctausu.COD_PRP ) left join silverdev.default.dm_tip_prd tipprd on (tipprd.COD_TIP_PRD = t.COD_TIP_PRD) left join silverdev.default.dm_pod pmov on (pmov.DAT = cast(t.DAT_MOV_AUT as date)) left join silverdev.default.dm_pod pgmt on (pgmt.DAT = cast(t.DAT_GMT as date)) left join silverdev.default.dm_pod paut on (paut.DAT = cast(t.DAT_AUT as date)) left join silverdev.default.dm_pod psol on (psol.DAT = cast(t.DAT_SOL as date)) left join silverdev.default.dm_pod prepas on (prepas.DAT = cast(t.DAT_REP as date))")

# COMMAND ----------

# DBTITLE 1,Add coluna PK
x = Window().orderBy(lit('A'))
insert_Transacao_Usuario_df = insert_Transacao_Usuario_df.withColumn("SRK_TRN", row_number().over(x))

# COMMAND ----------

# DBTITLE 1,Escreve na camada Silver
insert_Transacao_Usuario_df.write.insertInto("silverdev.default.f_Trn_Usu", overwrite=True)

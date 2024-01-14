-- Databricks notebook source
CREATE OR REPLACE VIEW GOLDDEV.DEFAULT.transacoes_usuarios AS
SELECT FTO.* FROM silverdev.default.f_trn_usu fto
INNER JOIN silverdev.default.dm_cli cli ON cli.SRK_CLI = fto.SRK_CLI
INNER JOIN silverdev.default.dm_est est ON est.SRK_ESTAB = fto.SRK_ESTAB
INNER JOIN silverdev.default.dm_tip_prd prd ON prd.SRK_TIP_PRD = fto.SRK_TIP_PRD
INNER JOIN silverdev.default.dm_usu usu ON usu.cod_prp_pf = fto.COD_PRP_PF
INNER JOIN silverdev.default.dm_pod mov ON mov.SRK_DAT = fto.SRK_DAT_MOV_AUT;

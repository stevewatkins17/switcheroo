SELECT TOP (10) [agid]
      ,[sessnum]
      ,[active]
      ,[epiid]
      ,[csvid]
      ,[visitstatus]
      ,[cpserviceid]
      ,[frequency]
      ,[details]
      ,[insertdate]
      ,[CarePlanHeaderID]
  FROM [dbo].[PC_CAREPLAN]


SELECT TOP (10) [agid]
      ,[sessnum]
      ,[active]
      ,[epiid]
      ,[csvid]
      ,[visitstatus]
      ,[cpserviceid]
      ,[frequency]
      ,[details]
      ,[insertdate]
      ,[CarePlanHeaderID]
  FROM [pcastage].[PC_CAREPLAN]

/*
SELECT TOP (10) [agid]
      ,[sessnum]
      ,[active]
      ,[epiid]
      ,[csvid]
      ,[visitstatus]
      ,[cpserviceid]
      ,[frequency]
      ,[details]
      ,[insertdate]
      ,[CarePlanHeaderID]
  FROM [pcamirror].[PC_CAREPLAN]

ALTER SCHEMA [pcastage] TRANSFER [dbo].[PC_CAREPLAN];
ALTER SCHEMA [dbo] TRANSFER [pcamirror].[PC_CAREPLAN];

ALTER SCHEMA [pcamirror] TRANSFER [dbo].[PC_CAREPLAN];
ALTER SCHEMA [dbo] TRANSFER [pcastage].[PC_CAREPLAN];v

--drop table if exists [dbo].[pca_log];

*/

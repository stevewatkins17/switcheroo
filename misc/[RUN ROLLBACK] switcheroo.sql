
exec [dbo].[usp_pca_switcheroo_orchestrator_rollback];

select * from [dbo].[pca_log] order by [rid] asc;

select 
   [ud_1]
  ,[PassFail]
  ,count(*) 
from [dbo].[pca_validation_log]
--where [PassFail] = N'Fail';
--where [ud_1] = N'table_column_ordinal';
group by    
 [ud_1]
,[PassFail]
order by [PassFail] asc;


/*
exec [dbo].[usp_pca_switcheroo_orchestrator_rollback]
   @source_schema = N'dbo'
  ,@stage_schema = N'pcastage'
  ,@mirror_schema = N'pcamirror';

*/

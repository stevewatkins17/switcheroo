/*
select distinct schema_name(schema_id) from sys.tables where is_ms_shipped = 0;

Accounting
archive
Billing
-- changedboceoa
dbo
DemandDenial
EDI
Falcon
Faxing
MAR
metadata
-- mirrordboceoa
patientGoals
PayorSources
PDGM
prlink
purge
settings
utility
*/

begin
set nocount on;

declare @original_schema nvarchar(128),@stage_schema nvarchar(128) ,@mirror_schema nvarchar(128);
declare @scoped_schemas table([rid] int identity(0,1) not null ,[SchemaName] nvarchar(128) not null);

/* insert into @scoped_schemas([SchemaName]) VALUES(N'dbo'); */
insert into @scoped_schemas([SchemaName])
select distinct [SchemaName] = schema_name(schema_id) from sys.tables where is_ms_shipped = 0
and (
      schema_name(schema_id) not like (N'%mirror%')
  and schema_name(schema_id) not like (N'%stage%')
  and schema_name(schema_id) not like (N'%change%')
  )
;

declare 
   @loopdex int = 0
  ,@maxdex int = (select max([rid]) + 1 from @scoped_schemas);

while @loopdex < @maxdex
begin
  select 
     @original_schema = [SchemaName] 
    ,@stage_schema = null --concat(N'tststage' ,[SchemaName]) -- 
    ,@mirror_schema = null --concat(N'tstmirror' ,[SchemaName]) -- 
  from @scoped_schemas where [rid] = @loopdex;

  print(concat(N'@original_schema: ' ,@original_schema));

  /**/
  exec [dbo].[usp_pca_switcheroo_orchestrator]
     @original_schema = @original_schema
    ,@project_code = N'tst'
    ,@withSwitch = 0
    ,@stage_schema = @stage_schema output
    ,@mirror_schema = @mirror_schema output;

  exec [dbo].[usp_pca_validation_orchestrator]
     @original_schema = @original_schema
    ,@stage_schema = @stage_schema
    ,@mirror_schema = @mirror_schema;

  set @loopdex += 1;

end
end

select * from [dbo].[pca_log] order by [rid] desc;

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

select 
*
from [dbo].[pca_validation_log]
where [PassFail] = N'Fail';

/*
table_definition
table_column_ordinal
XP
DF
ix_definition
Identity_Column
*/
/*
delete from [dbo].[pca_log];
delete from [dbo].[pca_validation_log];

exec [dbo].[usp_pca_switcheroo_orchestrator]
  @stage_schema = N'pcastage'
  ,@mirror_schema = N'pcamirror';

exec [dbo].[usp_pca_validation_orchestrator]
   @stage_schema = N'pcastage'
  ,@mirror_schema = N'pcamirror'

*/
/*
  select 
  [UD_1]
  ,count(*) 
  from [dbo].[pca_validation_log]
  group by [UD_1]
*/
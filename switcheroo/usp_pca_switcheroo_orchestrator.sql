SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO

create or alter procedure [dbo].[usp_pca_switcheroo_orchestrator]
   @original_schema nvarchar(128)
  ,@project_code nvarchar(128) = N''
  ,@withSwitch bit = 0
  ,@stage_schema nvarchar(128) output
  ,@mirror_schema nvarchar(128) output
as
begin
  set nocount on;
  declare @now datetimeoffset = sysdatetimeoffset();

  set @stage_schema = concat(@project_code ,N'stage'  ,@original_schema)
  set @mirror_schema = concat(@project_code ,N'mirror' ,@original_schema);

  insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg])
  values('session_begin' ,null ,null ,@now ,null); 

  insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg]) values
   ('original_schema' ,@original_schema ,null ,@now ,null) 
  ,('stage_schema' ,@stage_schema ,null ,@now ,null)
  ,('mirror_schema' ,@mirror_schema ,null ,@now ,null); 

  /* get PC "sessnum" -- table scope  */
  declare @expected_scoped_tables_count int = 115; 
  declare @actual_scoped_tables_count int; 

  declare @scoped_tables table ([rid] int identity(0,1) not null ,[SchemaName] nvarchar(128) not null ,[TableName] nvarchar(128) not null ,[ObjectID] int not null ,primary key([SchemaName] ,[TableName]));

  begin
    insert into @scoped_tables ([SchemaName] ,[TableName] ,[ObjectID])
    SELECT
       [SchemaName] = SCHEMA_NAME(s1.[schema_id])
      ,[TableName] = s1.[name]
      ,[ObjectID] = s1.[object_id]
        FROM sys.objects s1
        WHERE s1.[type] = 'U' AND 
        s1.[is_ms_shipped] = 0
        and SCHEMA_NAME(s1.[schema_id]) = @original_schema
        --AND s1.[name] NOT IN ('PC_CACHE_AGENTSPATIENTS_AP', 'PC_CACHE_PC_EPISODES_AP')
        AND s1.[name] NOT IN('pca_log' ,'pca_ixc_properties' ,'pca_ix_properties' ,'pca_validation_log')
        order by 1,2,3;
  end

  if @actual_scoped_tables_count <> @expected_scoped_tables_count
  begin
    insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg])
    select 
       [event_name] = 'unexpected_scope_count'
      ,[obj_schema_name] = null
      ,[obj_name] = null
      ,[insert_ts] = @now
      ,[msg] = concat('actual_scoped_tables_count: ' ,@actual_scoped_tables_count ,' expected_scoped_tables_count: ' ,@expected_scoped_tables_count)
  end

  insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg])
  select 
     [event_name] = 'in-scope'
    ,[obj_schema_name] = st.[SchemaName]
    ,[obj_name] = st.[TableName]
    ,[insert_ts] = @now
    ,[msg] = null
  from @scoped_tables st;

  declare 
     @ridex int = 0
    ,@end int = (select max([rid])+1 from @scoped_tables)
    ,@source_schema nvarchar(128)
    ,@source_name nvarchar(128);

  while @ridex < @end
  begin
    set @source_schema = null;
    set @source_name = null;

    select 
       @source_schema = [SchemaName]
      ,@source_name = [TableName]
    from @scoped_tables x   
    where x.rid = @ridex;

    exec [dbo].[usp_pca_switcheroo_controller]
       @source_schema = @source_schema
      ,@source_name = @source_name
      ,@stage_schema = @stage_schema
      ,@mirror_schema = @mirror_schema

    insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg])
      values('switcheroo' ,@source_schema ,@source_name ,@now ,null); 

    set @ridex += 1;
  end

  insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg])
  values('session_end' ,null ,null ,sysdatetimeoffset() ,null); 

end

/*
select * from [dbo].[pca_log]

declare
   @source_schema nvarchar(128) = N'dbo'
  ,@source_name nvarchar(128) = N'PC_PATIENTCONTACTINFO' --N'PC_PATIENTS1'
  ,@stage_schema nvarchar(128) = N'pcastage'
  ,@mirror_schema nvarchar(128) = N'pcamirror'
  ,@isRollback bit = 0;


exec [dbo].[usp_pca_switcheroo_controller]
   @source_schema = @source_schema
  ,@source_name = @source_name
  ,@stage_schema = @stage_schema
  ,@mirror_schema = @mirror_schema
  ,@isRollback = @isRollback;

*/

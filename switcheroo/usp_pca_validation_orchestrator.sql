SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO
create or alter procedure [dbo].[usp_pca_validation_orchestrator]
   @original_schema nvarchar(128) = N'dbo'
  ,@stage_schema nvarchar(128) = N'pcastage'
  ,@mirror_schema nvarchar(128) = N'pcamirror'
as
begin
  set nocount on;
  declare @now datetimeoffset = sysdatetimeoffset();
  
  if object_id('[dbo].[pca_log]') is null
  begin
    create table [dbo].[pca_log](
	     [rid] int identity(0,1) not null 
      ,[event_name] [nvarchar](32) not null
	    ,[obj_schema_name] [nvarchar](128) null
	    ,[obj_name] [nvarchar](128) null
      ,[insert_ts] datetimeoffset not null
      ,[msg] [nvarchar](4000) null
      );
  end

  insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg])
  values('validation_session_begin' ,null ,null ,@now ,null); 

  insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg]) values
   ('original_schema' ,@original_schema ,null ,@now ,null)
  ,('stage_schema' ,@stage_schema ,null ,@now ,null)
  ,('mirror_schema' ,@mirror_schema ,null ,@now ,null); 

  /* get PC "sessnum" -- table scope  */
  declare @expected_scoped_tables_count int = 115; 
  declare @actual_scoped_tables_count int; 

  declare @scoped_tables table ([rid] int identity(0,1) not null ,[SchemaName] nvarchar(128) not null ,[TableName] nvarchar(128) not null ,[ObjectID] int not null ,[PassFail] nchar(4) null ,primary key([SchemaName] ,[TableName]));

  begin
    INSERT INTO @scoped_tables ([SchemaName] ,[TableName] ,[ObjectID])
    SELECT 
       [SchemaName] = SCHEMA_NAME(s1.[schema_id])
      ,[TableName] = s1.[name]
      ,[ObjectID] = s1.[object_id]
    FROM sys.objects s1
    WHERE s1.[type] = 'U' AND 
    s1.[is_ms_shipped] = 0
    AND SCHEMA_NAME(s1.[schema_id]) = @original_schema
    AND s1.[name] NOT IN('pca_log' ,'pca_validation_log')
    ORDER BY 1,2,3;
  end

  select @actual_scoped_tables_count = count(*) from @scoped_tables;

  if @actual_scoped_tables_count <> @expected_scoped_tables_count
  begin
    insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg])
    select 
       [event_name] = 'validation_unexpected_scope_count'
      ,[obj_schema_name] = null
      ,[obj_name] = null
      ,[insert_ts] = @now
      ,[msg] = concat('validation_actual_scoped_tables_count: ' ,@actual_scoped_tables_count ,' validation_expected_scoped_tables_count: ' ,@expected_scoped_tables_count)
  end

  insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg])
  select 
     [event_name] = 'validation_in-scope'
    ,[obj_schema_name] = st.[SchemaName]
    ,[obj_name] = st.[TableName]
    ,[insert_ts] = @now
    ,[msg] = null
  from @scoped_tables st;

  declare 
     @ridex int = 0
    ,@end int = (select max([rid])+1 from @scoped_tables)
    ,@source_schema nvarchar(128)
    ,@source_name nvarchar(128)
    ,@local_PassFail nchar(4)
    
  declare @local_results table([val_type] nvarchar(128) ,[PassFail] nchar(4));

  
  while @ridex < @end
  begin
    set @source_schema = null;
    set @source_name = null;
    delete from @local_results;

    select 
       @source_schema = [SchemaName]
      ,@source_name = [TableName]
    from @scoped_tables x   
    where x.rid = @ridex;

    set @local_PassFail = null;  
    exec [dbo].[usp_pca_validate_table_definition]
       @source_schema = @source_schema 
      ,@source_name = @source_name
      ,@target_schema = @mirror_schema
      ,@PassFail= @local_PassFail output;
    insert @local_results values(N'table_definition' ,@local_PassFail);

    set @local_PassFail = null;  
    exec [dbo].[usp_pca_validate_column_definition_all]
       @source_schema = @source_schema 
      ,@source_name = @source_name
      ,@target_schema = @mirror_schema
      ,@PassFail= @local_PassFail output;
    insert @local_results values(N'table_column_definition' ,@local_PassFail);

    set @local_PassFail = null;
    exec [dbo].[usp_pca_validate_table_column_ordinal]
       @source_schema = @source_schema 
      ,@source_name = @source_name
      ,@target_schema = @mirror_schema
      ,@PassFail= @local_PassFail output;
    insert @local_results values(N'table_column_ordinal' ,@local_PassFail);

    set @local_PassFail = null;  
    exec [dbo].[usp_pca_validate_df_all]
       @source_schema = @source_schema 
      ,@source_name = @source_name
      ,@target_schema = @mirror_schema
      ,@PassFail= @local_PassFail output;
    insert @local_results values(N'DF' ,@local_PassFail);

    /* known issue with XP against an HCHB user table */
    set @local_PassFail = null;  
    exec [dbo].[usp_pca_validate_xp]
       @source_schema = @source_schema 
      ,@source_name = @source_name
      ,@target_schema = @mirror_schema
      ,@PassFail= @local_PassFail output;
    insert @local_results values(N'XP' ,@local_PassFail);

    set @local_PassFail = null;  
    exec [dbo].[usp_pca_validate_identity_column]
       @source_schema = @source_schema 
      ,@source_name = @source_name
      ,@target_schema = @mirror_schema
      ,@PassFail= @local_PassFail output;
    insert @local_results values(N'Identity_Column' ,@local_PassFail);

    set @local_PassFail = null;
    exec [dbo].[usp_pca_validate_ix_all]
       @source_schema = @source_schema 
      ,@source_name = @source_name
      ,@target_schema = @mirror_schema
      ,@PassFail= @local_PassFail output;
    insert @local_results values(N'ix_definition' ,@local_PassFail);

    if (select count(*) from @local_results where [PassFail] = N'Fail') > 0
    begin
      update @scoped_tables
      set [PassFail] = N'Fail';

    insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg])
      values('ERROR: Validation Fail' ,@source_schema ,@source_name ,@now ,'details in dbo.pca_validation_log'); 
    end
    else
    begin
      update @scoped_tables
      set [PassFail] = N'Pass';

      insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg])
        values('Validation Pass' ,@source_schema ,@source_name ,@now ,null); 
    end

    set @ridex += 1;
  end

  insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg])
  values('validation_session_end' ,null ,null ,sysdatetimeoffset() ,null); 

end
/*
go
exec [dbo].[usp_pca_validation_orchestrator];

select * from [dbo].[pca_log]

select * from [dbo].[pca_validation_log]
where [PassFail] = N'Fail'
and [ud_0] > '2024-11-08 16:50:00.0000000 -05:00'
*/



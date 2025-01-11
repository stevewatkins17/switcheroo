SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO
create or alter procedure [dbo].[usp_pca_validate_identity_column]
   @source_schema nvarchar(128)
  ,@source_name nvarchar(128) 
  ,@target_schema nvarchar(128)
  ,@PassFail nchar(4) output
as
begin
set nocount on;

declare @now datetimeoffset = sysdatetimeoffset();
declare @target_name varchar(128) = @source_name;

declare 
     @source_objid int = object_id(concat(@source_schema,N'.',@source_name))-- (select [object_id] from sys.tables o join sys.schemas s on o.[schema_id] = s.[schema_id] and s.[name] = @source_schema and o.name = @source_name)
    ,@target_objid int = object_id(concat(@target_schema,N'.',@target_name))-- (select [object_id] from sys.tables o join sys.schemas s on o.[schema_id] = s.[schema_id] and s.[name] = @target_schema and o.name = @target_name);

declare @ic_count tinyint = (
  select count(*) from [sys].[identity_columns] ic
    where ic.[object_id] in(@source_objid ,@target_objid)
);

declare 
   @delta_compare int
  ,@isEQGL_target_ident_pass bit;


if @ic_count = 1 begin set @PassFail = N'Fail' end;

if @ic_count = 2 
begin
  declare @identity_column_name nvarchar(128) = (
    select COL_NAME(ic.[object_id] ,ic.[column_id]) from [sys].[identity_columns] ic
    where ic.[object_id] in(@source_objid)
    );

  declare @source_ident_current bigint = IDENT_CURRENT(@source_schema + '.' + @source_name);
  declare @target_ident_current bigint = IDENT_CURRENT(@target_schema + '.' + @source_name);

  declare @source_maxID bigint;
  declare @sql_param nvarchar(4000) = '@source_maxID bigint output';
  declare @sql nvarchar(4000) = concat(N'select @source_maxID = max([',@identity_column_name ,N']) from [' ,@source_schema ,N'].[' ,@source_name ,N']');

  exec sys.sp_executesql @sql, @sql_param ,@source_maxID output;

  select @isEQGL_target_ident_pass = (case 
    when @source_maxID is null and @target_ident_current = @source_ident_current
    then 1
    when @source_maxID is not null and (@target_ident_current >= @source_ident_current or @target_ident_current >= @source_maxID)
    then 1
    else 0 end);

  with cte as(
  select 
     /*[object_id]
    ,*/[name]
    /*,[column_id]
    ,[system_type_id]
    ,[user_type_id]
    ,[max_length]
    ,[precision]
    ,[scale]*/
    ,[collation_name]
    ,[is_nullable]
    ,[is_ansi_padded]
    ,[is_rowguidcol]
    ,[is_identity]
    ,[is_filestream]
    ,[is_replicated]
    ,[is_non_sql_subscribed]
    ,[is_merge_published]
    ,[is_dts_replicated]
    ,[is_xml_document]
    ,[xml_collection_id]
    ,[default_object_id]
    ,[rule_object_id]
    ,[seed_value]
    ,[increment_value]
    --,[last_value]
    ,[is_not_for_replication]
    ,[is_computed]
    ,[is_sparse]
    ,[is_column_set]
    ,[generated_always_type]
    ,[generated_always_type_desc]
    ,[encryption_type]
    ,[encryption_type_desc]
    ,[encryption_algorithm_name]
    ,[column_encryption_key_id]
    ,[column_encryption_key_database_name]
    ,[is_hidden]
    ,[is_masked]
    ,[graph_type]
    ,[graph_type_desc]
  from [sys].[identity_columns] ic
  where ic.[object_id] = @source_objid
  except
  select 
     /*[object_id]
    ,*/[name]
    /*,[column_id]
    ,[system_type_id]
    ,[user_type_id]
    ,[max_length]
    ,[precision]
    ,[scale]*/
    ,[collation_name]
    ,[is_nullable]
    ,[is_ansi_padded]
    ,[is_rowguidcol]
    ,[is_identity]
    ,[is_filestream]
    ,[is_replicated]
    ,[is_non_sql_subscribed]
    ,[is_merge_published]
    ,[is_dts_replicated]
    ,[is_xml_document]
    ,[xml_collection_id]
    ,[default_object_id]
    ,[rule_object_id]
    ,[seed_value]
    ,[increment_value]
    --,[last_value]
    ,[is_not_for_replication]
    ,[is_computed]
    ,[is_sparse]
    ,[is_column_set]
    ,[generated_always_type]
    ,[generated_always_type_desc]
    ,[encryption_type]
    ,[encryption_type_desc]
    ,[encryption_algorithm_name]
    ,[column_encryption_key_id]
    ,[column_encryption_key_database_name]
    ,[is_hidden]
    ,[is_masked]
    ,[graph_type]
    ,[graph_type_desc]
  from [sys].[identity_columns] ic
  where ic.[object_id] = @target_objid
  union
  select 
     /*[object_id]
    ,*/[name]
    /*,[column_id]
    ,[system_type_id]
    ,[user_type_id]
    ,[max_length]
    ,[precision]
    ,[scale]*/
    ,[collation_name]
    ,[is_nullable]
    ,[is_ansi_padded]
    ,[is_rowguidcol]
    ,[is_identity]
    ,[is_filestream]
    ,[is_replicated]
    ,[is_non_sql_subscribed]
    ,[is_merge_published]
    ,[is_dts_replicated]
    ,[is_xml_document]
    ,[xml_collection_id]
    ,[default_object_id]
    ,[rule_object_id]
    ,[seed_value]
    ,[increment_value]
    --,[last_value]
    ,[is_not_for_replication]
    ,[is_computed]
    ,[is_sparse]
    ,[is_column_set]
    ,[generated_always_type]
    ,[generated_always_type_desc]
    ,[encryption_type]
    ,[encryption_type_desc]
    ,[encryption_algorithm_name]
    ,[column_encryption_key_id]
    ,[column_encryption_key_database_name]
    ,[is_hidden]
    ,[is_masked]
    ,[graph_type]
    ,[graph_type_desc]
  from [sys].[identity_columns] ic
  where ic.[object_id] = @target_objid
  except
  select 
     /*[object_id]
    ,*/[name]
    /*,[column_id]
    ,[system_type_id]
    ,[user_type_id]
    ,[max_length]
    ,[precision]
    ,[scale]*/
    ,[collation_name]
    ,[is_nullable]
    ,[is_ansi_padded]
    ,[is_rowguidcol]
    ,[is_identity]
    ,[is_filestream]
    ,[is_replicated]
    ,[is_non_sql_subscribed]
    ,[is_merge_published]
    ,[is_dts_replicated]
    ,[is_xml_document]
    ,[xml_collection_id]
    ,[default_object_id]
    ,[rule_object_id]
    ,[seed_value]
    ,[increment_value]
    --,[last_value]
    ,[is_not_for_replication]
    ,[is_computed]
    ,[is_sparse]
    ,[is_column_set]
    ,[generated_always_type]
    ,[generated_always_type_desc]
    ,[encryption_type]
    ,[encryption_type_desc]
    ,[encryption_algorithm_name]
    ,[column_encryption_key_id]
    ,[column_encryption_key_database_name]
    ,[is_hidden]
    ,[is_masked]
    ,[graph_type]
    ,[graph_type_desc]
  from [sys].[identity_columns] ic
  where ic.[object_id] = @source_objid
  )
  select @delta_compare = count(*) from cte;

  select @PassFail = case 
    when @delta_compare = 0 and @isEQGL_target_ident_pass = 1
    then N'Pass' else 'Fail' end;

end

insert into [dbo].[pca_validation_log]([i],[db],[ud_0],[ud_1],[ud_2],[ud_3],[PassFail])
select 
    [i] = @@servername
  ,[db] = db_name() 
  ,[ud_0] = @now
  ,[ud_1] = N'Identity_Column'
  ,[ud_2] = concat(N'target: ' ,@target_schema ,N'.',@target_name ,N', source: ' ,@source_schema ,N'.',@source_name) 
  ,[ud_3] = concat(N'isEQGL_target_ident_current: ' ,@isEQGL_target_ident_pass  ,N', delta_compare: ' ,@delta_compare) 
  ,[PassFail] = @PassFail;

end

/*
go
--target: dbo.PC_CLINICIANETATRACKING, source: pcastage.PC_CLINICIANETATRACKING
--target: dbo.PC_PATIENTMEDICATIONSETUP, source: pcastage.PC_PATIENTMEDICATIONSETUP

declare 
   @source_schema nvarchar(128) = N'pcastage' 
  ,@source_name nvarchar(128) = N'PC_CLINICIANETATRACKING' --N'PC_CLINICIANETATRACKING' -- N'PC_PATIENTS1' -- DBCC CHECKIDENT('[dbo].[PC_ELECTIONADDENDUMREQUESTEDSTATUS]', RESEED ,223485);
  ,@target_schema nvarchar(128) = N'dbo'

declare @PassFail nchar(4);

exec [dbo].[usp_pca_validate_identity_column]
   @source_schema = @source_schema 
  ,@source_name = @source_name
  ,@target_schema = @target_schema
  ,@PassFail= @PassFail output;

select [@PassFail] = @PassFail;

select * from [dbo].[pca_validation_log];

select [@target_ident_current] = IDENT_CURRENT(@target_schema + N'.PC_CLINICIANETATRACKING'); 

*/
/*
declare 
   @source_schema nvarchar(128) = N'pcastage' 
  ,@source_name nvarchar(128) = N'PC_CLINICIANETATRACKING' --N'PC_CLINICIANETATRACKING' -- N'PC_PATIENTS1' -- DBCC CHECKIDENT('[dbo].[PC_ELECTIONADDENDUMREQUESTEDSTATUS]', RESEED ,223485);
  ,@target_schema nvarchar(128) = N'dbo'

select [@source_ident_current] = IDENT_CURRENT(@source_schema + N'.PC_CLINICIANETATRACKING'); 
select [@target_ident_current] = IDENT_CURRENT(@target_schema + N'.PC_CLINICIANETATRACKING'); 
select [@source_current_ident_SEED] = IDENT_SEED(@source_schema + N'.PC_CLINICIANETATRACKING'); 
select [@target_current_ident_SEED] = IDENT_SEED(@target_schema + N'.PC_CLINICIANETATRACKING'); 
select [@source_current_ident_INCR] = IDENT_INCR(@source_schema + N'.PC_CLINICIANETATRACKING'); 
select [@target_current_ident_INCR] = IDENT_INCR(@target_schema + N'.PC_CLINICIANETATRACKING'); 

--select [@source_ident_current] = IDENT_CURRENT(@source_schema + '.' + @source_name); 
--select [@target_ident_current] = IDENT_CURRENT(@target_schema + '.' + @source_name); 
--select [@source_current_ident_SEED] = IDENT_SEED(@source_schema + '.' + @source_name); 
--select [@target_current_ident_SEED] = IDENT_SEED(@target_schema + '.' + @source_name); 
--select [@source_current_ident_INCR] = IDENT_INCR(@source_schema + '.' + @source_name); 
--select [@target_current_ident_INCR] = IDENT_INCR(@target_schema + '.' + @source_name); 

DBCC CHECKIDENT('[dbo].[PC_ELECTIONADDENDUMREQUESTS]', RESEED ,31464);
DBCC CHECKIDENT('[dbo].[PC_PATIENTCONTACTINFO]', RESEED ,-2147456251);
DBCC CHECKIDENT('[dbo].[PC_POINTCAREIPV4LOGS]', RESEED ,-2123548986);
DBCC CHECKIDENT('[dbo].[PC_POINTCARECONNECTIVITYLOGS]', RESEED ,-2132322484);
DBCC CHECKIDENT('[dbo].[PC_ELECTIONADDENDUMREQUESTCONTACT]', RESEED ,31464);
DBCC CHECKIDENT('[dbo].[PC_ELECTIONADDENDUMREQUESTEDSTATUS]', RESEED ,223485);
*/
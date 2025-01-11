SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO
create or alter proc [dbo].[usp_pca_create_df]
   @source_schema nvarchar(128) = N'{source_schema}' 
  ,@source_name nvarchar(128) = N'{source_name}' 
  ,@target_schema nvarchar(128) = N'{target_schema}' 
  ,@constraint_name nvarchar(128) = N'{df_name}'
  ,@stmt_create [nvarchar](max) output
as
begin
  set nocount on;

  declare @target_name nvarchar(128) = @source_name;

  /* we begin creating */
  declare @source_objid int = (select [object_id] from sys.objects o join sys.schemas s on o.[schema_id] = s.[schema_id] and s.[name] = @source_schema and o.name = @source_name);

  declare
    @constraint_schema_name [nvarchar](128),
    @parent_schema_name [nvarchar](128),
    @parent_obj_name [nvarchar](128) ,
    @parent_column_name [nvarchar](128),
    @is_system_named [bit],
    @definition [nvarchar](max);
        
  select 
       @constraint_schema_name = SCHEMA_NAME(cc.[schema_id])
      ,@parent_schema_name = OBJECT_SCHEMA_NAME(cc.[parent_object_id])
      ,@parent_obj_name = object_name(parent_object_id)
      ,@parent_column_name = COL_NAME(parent_object_id , parent_column_id)
      ,@definition = OBJECT_DEFINITION(cc.[object_id])
      ,@is_system_named = cc.[is_system_named]
  from sys.default_constraints cc
  where cc.[is_ms_shipped] = 0 
  and cc.parent_object_id = @source_objid
  and cc.[name] = @constraint_name;

  set @definition = (
    CASE WHEN LEFT(@definition, 1) = '(' AND RIGHT(@definition, 1) = ')'
         THEN SUBSTRING(@definition, 2, LEN(@definition) - 2)
         ELSE @definition END
    );

  set @stmt_create  = N'
  ALTER TABLE [' + @target_schema + N'].[' + @target_name + N'] DROP CONSTRAINT if exists [' + @constraint_name + N'];
  ';

  if @is_system_named = 0
  begin
    set @stmt_create += N'
  ALTER TABLE [' + @target_schema + N'].[' + @target_name + N'] ADD CONSTRAINT [' + @constraint_name + N'] DEFAULT ' + @definition + N' for [' + @parent_column_name + N'];
  ';
  end
  else
  begin
    set @stmt_create += N'
  ALTER TABLE [' + @target_schema + N'].[' + @target_name + N'] ADD DEFAULT ' + @definition + N' for [' + @parent_column_name + N'];
  ';
  end

end

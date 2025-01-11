SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO
create or alter procedure [dbo].[usp_pca_create_xp]
   @source_schema nvarchar(128) = N'dbo' 
  ,@source_name nvarchar(128) = N'PC_PATIENTS1'
  ,@target_schema nvarchar(128) = N'pcamirror'
  ,@class tinyint = 1 
  ,@minor_name nvarchar(128) = N'VisitSourceId'
  ,@stmt_base_drop nvarchar(max) output
  ,@stmt_base_add nvarchar(max) output
as
begin
/* we begin creating */
declare @source_objid int = (select [object_id] from sys.tables o join sys.schemas s on o.[schema_id] = s.[schema_id] and s.[name] = @source_schema and o.name = @source_name);

declare 
   @name nvarchar(128)
  ,@value nvarchar(max)
--  ,@stmt_create nvarchar(max)
  ,@stmt_minor_name_filter nvarchar(4000);

declare @level2type nvarchar(128) = (
  case 
    when @class = 1 and len(@minor_name) > 0 then N'COLUMN'
    when @class = 7 then N'INDEX'
  end
);

if @class = 1 
begin
  select 
    @name = extp.[name]
   ,@value = replace((convert(nvarchar(max) ,extp.[value])) ,'''' ,'''''') 
  from sys.[extended_properties] extp
  where 1=1 
  and extp.[class] = @class
  and extp.[major_id] = @source_objid
  and (
          (@level2type is not null and COL_NAME(extp.[major_id] ,extp.[minor_id]) = @minor_name)
      or  (@level2type is null and extp.[minor_id] = 0)
      )
end

if @class = 7
begin
  select 
    @name = extp.[name]
    ,@value = replace((convert(nvarchar(max) ,extp.[value])) ,'''' ,'''''') 
  from sys.[extended_properties] extp
  join sys.indexes i on i.[object_id] = @source_objid and i.[name] = @minor_name
    and extp.[class] = @class
    and extp.[major_id] = @source_objid;   
end

if @class =  1 
begin
  if @level2type is not null 
  begin 
    set @stmt_minor_name_filter = N'and COL_NAME(extp.[major_id] ,extp.[minor_id]) = ''' + @minor_name + N''''
  end
  else
  begin 
    set @stmt_minor_name_filter = N'and extp.[minor_id] = 0 '
  end
end

if @class =  7
begin 
  set @stmt_minor_name_filter = 'and extp.[minor_id] = (select i.[index_id] from sys.indexes i where i.[object_id] = extp.[major_id] and i.[name] = ''' + @minor_name + N''')'
end

set @stmt_base_drop = N'
/* begin extended property: '+ @target_schema + N'.' + @source_name +N' '+ isnull(@minor_name,'') + N' */
  if exists(
    select 1 from sys.extended_properties extp
    where extp.[class] = ' + convert(nvarchar(4), @class) + N'
    and extp.[major_id] = object_id(''' + @target_schema + N'.' + @source_name + N''')
    ' + @stmt_minor_name_filter + N'
  )
  begin
    EXEC sys.sp_dropextendedproperty @name=N''' + @name + N''', @level0type=N''SCHEMA'',@level0name=N'''+ @target_schema + N''', @level1type=N''TABLE'',@level1name=N''' + @source_name + ''''
;

set @stmt_base_add = N'
  EXEC sys.sp_addextendedproperty @name=N''' + @name + N''', @value=N'''+ @value + N''' , @level0type=N''SCHEMA'',@level0name=N'''+ @target_schema + N''', @level1type=N''TABLE'',@level1name=N''' + @source_name + ''''
;

if @level2type is not null
begin
  set @stmt_base_drop += N' ,@level2type=N'''+ @level2type + N''',@level2name=N'''+ @minor_name + N''''
  set @stmt_base_add  += N' ,@level2type=N'''+ @level2type + N''',@level2name=N'''+ @minor_name + N''''
end

set @stmt_base_drop += N';
  end;
';

set @stmt_base_add  += N'; 
/* end extended property: '+ @target_schema + N'.' + @source_name +' '+ isnull(@minor_name,'') + ' */
';

end

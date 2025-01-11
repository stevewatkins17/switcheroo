SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE or ALTER PROCEDURE [dbo].[usp_pca_reset_seed](
     @source_schema nvarchar(128)
    ,@source_name nvarchar(128)
    ,@mirror_schema nvarchar(128)
    ,@stmt nvarchar(max) output
) 
AS 
BEGIN 
  declare @source_objid int = (select [object_id] from sys.tables o join sys.schemas s on o.[schema_id] = s.[schema_id] and s.[name] = @source_schema and o.name = @source_name);

  declare @source_ident_current bigint = IDENT_CURRENT(@source_schema + '.' + @source_name);
  declare @next_seed bigint;

  declare @id_column nvarchar(128) = (
    select [name]
    from sys.identity_columns ic
    where ic.[object_id] = @source_objid
  );

  declare @next_max_ID bigint;
  declare @sql_param nvarchar(4000) = N'@next_max_ID bigint output';
  declare @sql_exe nvarchar(max) = N'
    select @next_max_ID = max('+ @id_column +') +1 from ['+ @source_schema +'].['+ @source_name +'];
  ';

  exec sys.sp_executesql @sql_exe ,@sql_param ,@next_max_ID = @next_max_ID output;

  set @next_seed = case when isnull(@next_max_ID ,-9223372036854775808) > isnull(@source_ident_current ,-9223372036854775808) then @next_max_ID else @source_ident_current end 
  select @stmt = case 
    when @next_seed is not null 
    then concat('DBCC CHECKIDENT(''[' ,@mirror_schema , N'].[' , @source_name , N']'', RESEED ,' , @next_seed , ');')
    else null end;

end
/*
go

-- target: tstmirrordbo.pca_log, source: dbo.pca_log	isEQGL_target_ident_current: 0, delta_compare: 0
declare 
   @source_schema nvarchar(128) = N'dbo'
  ,@source_name nvarchar(128) = N'pca_log'
  ,@mirror_schema nvarchar(128) = N'tstmirrordbo'
  ,@stmt nvarchar(max);

exec [dbo].[usp_pca_reset_seed]
     @source_schema = @source_schema
    ,@source_name = @source_name
    ,@mirror_schema = @mirror_schema
    ,@stmt = @stmt output

print(@stmt)


*/
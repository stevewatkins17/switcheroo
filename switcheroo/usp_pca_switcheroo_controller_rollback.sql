SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO
create or alter procedure [dbo].[usp_pca_switcheroo_controller_rollback]
   @source_schema nvarchar(128)
  ,@source_name nvarchar(128)
  ,@stage_schema nvarchar(128)
  ,@mirror_schema nvarchar(128)
as
begin
  set nocount on;

  if object_id(concat(@source_schema,N'.',@source_name)) is null
  begin
    throw 50017, 'ERROR: source dbo.table not exist', 1;
  end

  if object_id(concat(@mirror_schema,N'.',@source_name)) is not null
  begin
    throw 50027, 'ERROR: mirror.table pre-exists', 1;
  end

  declare @run_stmt nvarchar(max);

  if   object_id(concat(@stage_schema ,N'.',@source_name)) is null 
    or object_id(concat(@source_schema,N'.',@source_name)) is null
    or object_id(concat(@mirror_schema,N'.',@source_name)) is not null
  begin
    throw 50037, 'ERROR: tables not in correct schemas for rollback', 1;
  end

  /* rollback dbo-to-mirror */
  set @run_stmt = ([dbo].[fn_pca_schema_xfer](@source_schema, @mirror_schema ,@source_name));
  exec sp_executesql @run_stmt;

  /* rollback stage-to-dbo */
  set @run_stmt = ([dbo].[fn_pca_schema_xfer](@stage_schema ,@source_schema ,@source_name));
  exec sp_executesql @run_stmt;

end

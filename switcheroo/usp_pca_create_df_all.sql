SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO
create or alter procedure [dbo].[usp_pca_create_df_all]
   @source_schema nvarchar(128) = N'{source_schema}' 
  ,@source_name nvarchar(128) = N'{source_name}'
  ,@target_schema nvarchar(128) = N'{target_schema}' 
  ,@create_DF_batch nvarchar(max) output
as
begin
  set nocount on;

  declare @source_objid int = object_id(@source_schema + '.' + @source_name);

  declare @DF table([rid] int identity(0,1) ,[constraint_name] nvarchar(128));

  insert into @DF([constraint_name])
  select [name]
  from sys.default_constraints cc
  where cc.[is_ms_shipped] = 0 
  and cc.parent_object_id = @source_objid;

  declare 
     @ridex int = 0
    ,@end int = (select max([rid])+1 from @DF)
    ,@constraint_name nvarchar(128)
    ,@stmt_DF nvarchar(4000);

  set @create_DF_batch = N''; 

  while @ridex < @end
  begin
    set @stmt_DF = null;
    set @constraint_name = null;

    select @constraint_name = [constraint_name]
    from @DF x   
    where x.rid = @ridex;

    exec [dbo].[usp_pca_create_df]
       @source_schema = @source_schema
      ,@source_name = @source_name
      ,@target_schema = @target_schema
      ,@constraint_name = @constraint_name
      ,@stmt_create = @stmt_DF output;

    set @create_DF_batch += @stmt_DF;
    set @ridex += 1;
  end
end

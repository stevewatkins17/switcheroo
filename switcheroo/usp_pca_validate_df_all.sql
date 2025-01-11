/* validate list of all indexes for a table */
SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO
create or alter procedure [dbo].[usp_pca_validate_df_all]
   @source_schema nvarchar(128) 
  ,@source_name nvarchar(128)
  ,@target_schema nvarchar(128)
  ,@PassFail nchar(4) output
as
begin
  set nocount on;

  declare @now datetimeoffset = sysdatetimeoffset();
  declare @target_name varchar(128) = @source_name;
  declare @source_objid int = (select [object_id] from sys.objects o join sys.schemas s on o.[schema_id] = s.[schema_id] and s.[name] = @source_schema and o.name = @source_name);

  declare @df_list table([rid] int identity(0,1) not null ,[name] nvarchar(128) not null ,[LocalPassFail] nchar(4) null);

  insert into @df_list([name])
  select [name]
  from sys.default_constraints cc
  where cc.[is_ms_shipped] = 0 
  and cc.parent_object_id = @source_objid;

  declare 
     @ridex int = 0
    ,@end int = (select max([rid])+1 from @df_list)
    ,@source_df_name nvarchar(128)
    ,@Local_PassFail nchar(4);

  while @ridex < @end
  begin
    set @source_df_name = null;
    set @Local_PassFail = null;

    select @source_df_name = [name]
    from @df_list x   
    where x.rid = @ridex;

    exec [dbo].[usp_pca_validate_df]
     @source_schema = @source_schema 
    ,@source_name = @source_name
    ,@target_schema = @target_schema
    ,@source_df_name = @source_df_name
    ,@PassFail = @Local_PassFail output

    update @df_list 
    set [LocalPassFail] = @Local_PassFail
    where rid = @ridex;

    set @ridex += 1;
  end

  select 
    @PassFail = case when count(*) = 0 then N'Pass' else N'Fail' end 
  from @df_list 
  where [LocalPassFail] = N'Fail';

end
/*
go
declare @PassFail nchar(4);

exec [dbo].[usp_pca_validate_df_all]
   @source_schema = 'pcastage' 
  ,@source_name = 'PC_PATIENTS1'
  ,@target_schema = 'dbo'
  ,@PassFail= @PassFail output;

select [@PassFail] = @PassFail;

select * from [dbo].[pca_validation_log]

*/
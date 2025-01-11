SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO
create or alter procedure [dbo].[usp_pca_create_xp_all]
   @source_schema nvarchar(128) = N'dbo' 
  ,@source_name nvarchar(128) = N'PC_PATIENTS1'
  ,@target_schema nvarchar(128) = N'pcamirror'
  ,@stmt_batch nvarchar(max) output
as
begin
  set nocount on;

  declare @source_objid int = object_id(@source_schema + '.' + @source_name);

  declare @xp table([rid] int identity(0,1) ,[class] tinyint ,[minor_name] nvarchar(128));

  insert into @xp([class],[minor_name])
  select 
     extp.[class] 
    ,[minor_name] =  case 
        when extp.[class] = 1 then (isnull( (COL_NAME(extp.[major_id] ,extp.[minor_id])) ,''))
        when extp.[class] = 7 then (select i.[name] from sys.indexes i where i.[object_id] = @source_objid and i.[index_id] = extp.[minor_id])
      end
  from sys.[extended_properties] extp
  where extp.[class] in(1,7) 
  and extp.[major_id] = @source_objid;

  declare 
     @ridex int = 0
    ,@end int = (select max([rid])+1 from @xp)
    ,@class tinyint
    ,@minor_name nvarchar(128)
    ,@stmt_base_drop nvarchar(4000)
    ,@stmt_base_add nvarchar(4000);

  set @stmt_batch = N''; 

  while @ridex < @end
  begin
    set @class = null;
    set @minor_name = null;
    set @stmt_base_drop = null;
    set @stmt_base_add = null;

    select 
       @class = class
      ,@minor_name = minor_name
    from @xp x   
    where x.rid = @ridex;

    exec [dbo].[usp_pca_create_xp]
       @source_schema = @source_schema 
      ,@source_name = @source_name
      ,@target_schema = @target_schema 
      ,@class = @class 
      ,@minor_name = @minor_name 
      ,@stmt_base_drop = @stmt_base_drop output
      ,@stmt_base_add = @stmt_base_add output;

    set @stmt_batch += @stmt_base_drop + ' ' + @stmt_base_add;
    set @ridex += 1;
  end
end

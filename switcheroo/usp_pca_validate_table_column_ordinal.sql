SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO


create or alter procedure [dbo].[usp_pca_validate_table_column_ordinal]
   @source_schema nvarchar(128) = N'pcastage' 
  ,@source_name nvarchar(128) = N'PC_PATIENTS1'
  ,@target_schema nvarchar(128) = N'dbo'
  ,@PassFail nchar(4) output
as
begin
set nocount on;

declare @now datetimeoffset = sysdatetimeoffset();
declare @target_name varchar(128) = @source_name;

declare @include_repl bit = 0;

declare 
     @source_objid int = (select [object_id] from sys.tables o join sys.schemas s on o.[schema_id] = s.[schema_id] and s.[name] = @source_schema and o.name = @source_name)
    ,@target_objid int = (select [object_id] from sys.tables o join sys.schemas s on o.[schema_id] = s.[schema_id] and s.[name] = @target_schema and o.name = @target_name);

create table #proprty_compare(property varchar(max) ,p_value varchar(max));

    /* validate inputs */
    if len(@source_schema) > 128 or len(@source_name) > 128 
        or len(@target_schema) > 128 or len(@target_name) > 128 
    BEGIN
        RAISERROR('source or target obj name too big; length > 128', 11, 1)
    end

SELECT 
       c.[object_id]
      ,[name]
      ,[column_order] = ROW_NUMBER() OVER(PARTITION BY c.[object_id] ORDER BY [column_id] ASC)
into #temp_c
  FROM [sys].[columns] c
  where c.[object_id] in( @source_objid ,@target_objid)

/* to force failure testing, we alter properties for target or source
if 1=0
BEGIN
    update [#temp_c]
    set [column_order] = -99 
    where [object_id] = @target_objid
    and [column_order] =  12

END
*/

declare @distinct_tablename_count int;

with cte as(
    SELECT distinct([object_id]) as [object_id] FROM [#temp_c] 
)
select @distinct_tablename_count = count(*) from cte;

if @distinct_tablename_count <> 2
    BEGIN
      set @PassFail = N'Fail';
      
      insert into [dbo].[pca_validation_log]([i],[db],[ud_0],[ud_1],[ud_2],[ud_3],[PassFail])
      select 
         [i] = @@servername
        ,[db] = db_name() 
        ,[ud_0] = @now
          ,[ud_1] = N'table_column_ordinal'
          ,[ud_2] = concat('target: ' ,@target_schema ,'.',@target_name ,', source: ' ,@source_schema ,'.',@source_name) 
        ,[ud_3] = null
        ,[PassFail] = @PassFail
    END

else 
    begin
        insert into #proprty_compare
        select [name] ,[column_order] from #temp_c c
        where c.[object_id] = @source_objid
        except   
        select [name] ,[column_order] from #temp_c c
        where c.[object_id] = @target_objid

        insert into #proprty_compare
        select [name] ,[column_order] from #temp_c c
        where c.[object_id] = @target_objid
        except   
        select [name] ,[column_order] from #temp_c c
        where c.[object_id] = @source_objid

      declare @fail_record_count int = (select count(*) from #proprty_compare);
    
      select @PassFail = (case when @fail_record_count = 0 then N'Pass' Else N'Fail' end);

      insert into [dbo].[pca_validation_log]([i],[db],[ud_0],[ud_1],[ud_2],[ud_3],[PassFail])
      select 
          [i] = @@servername
          ,[db] = db_name() 
          ,[ud_0] = @now
          ,[ud_1] = N'table_column_ordinal'
          ,[ud_2] = concat(N'target: ' ,@target_schema ,N'.',@target_name ,N', source: ' ,@source_schema ,N'.',@source_name) 
          ,[ud_3] = null --concat([property] ,N': ',[p_value] ,N' ,@fail_record_count: ' ,@fail_record_count)
          ,[PassFail] = @PassFail;
    end
  END
/*
go
declare @PassFail nchar(4);

exec [dbo].[usp_pca_validate_table_column_ordinal]
   @source_schema = N'pcastage' 
  ,@source_name = N'PC_PATIENTS1'
  ,@target_schema = N'dbo'
  ,@PassFail= @PassFail output;

select [@PassFail] = @PassFail;

select * from [dbo].[pca_validation_log] 
order by [ud_0] desc
*/
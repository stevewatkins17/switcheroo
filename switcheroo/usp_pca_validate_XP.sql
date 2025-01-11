SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO

create or alter procedure [dbo].[usp_pca_validate_xp]
   @source_schema nvarchar(128) = 'pcastage' 
  ,@source_name nvarchar(128) = 'PC_PATIENTS1'
  ,@target_schema nvarchar(128) = 'dbo'
  ,@PassFail nchar(4) output
as
begin
set nocount on;

declare @now datetimeoffset = sysdatetimeoffset();
declare @target_name varchar(128) = @source_name;

declare 
     @source_objid int = (select [object_id] from sys.objects o join sys.schemas s on o.[schema_id] = s.[schema_id] and s.[name] = @source_schema and o.name = @source_name)
    ,@target_objid int = (select [object_id] from sys.objects o join sys.schemas s on o.[schema_id] = s.[schema_id] and s.[name] = @target_schema and o.name = @target_name);

select 
     ex.[class_desc]
    ,[table_name] = object_name(ex.[major_id])
    ,[column_name] = COL_NAME(ex.[major_id] , ex.[minor_id])
    ,ex.[name]
    ,ex.[value]
into [#source_xp]
from [sys].[extended_properties] ex
where ex.[major_id]in(@source_objid);

select 
     ex.[class_desc]
    ,[table_name] = object_name(ex.[major_id])
    ,[column_name] = COL_NAME(ex.[major_id] , ex.[minor_id])
    ,ex.[name]
    ,ex.[value]
into [#target_xp]
from [sys].[extended_properties] ex
where ex.[major_id]in(@target_objid);


/* step 2 - we evaluate the output of the 2 holding #tables and return test results */
begin 
    declare @countdelta int ,@delta0 int ,@delta1 int ,@V0count int ,@V1count int; 

    with cte as(
        select 
         [V0 count] = (select count(*) from #source_xp)
        ,[V1 count] = (select count(*) from #target_xp)
    )
    select 
         @V0count = c.[V0 count]
        ,@V1count = c.[V1 count]
        ,@countdelta = (c.[V0 count] - c.[V1 count])
    from cte c; 

    with cte as(
        select * from #source_xp
        except
        select * from #target_xp
    )
    select @delta0 = count(*) from cte c;

    with cte as(
        select * from #target_xp
        except
        select * from #source_xp
    )
    select @delta1 = count(*) from cte c;

    select @PassFail = (case 
          when @V0count = 0 and @V1count = 0 then null
          when (@V0count - @V1count = 0) and (@delta0 + @delta1 = 0) then N'Pass' 
          else N'Fail' end);

    insert into [dbo].[pca_validation_log]([i],[db],[ud_0],[ud_1],[ud_2],[ud_3],[PassFail])
    select 
       [i] = @@servername
      ,[db] = db_name() 
      ,[ud_0] = @now
      ,[ud_1] = N'XP'
      ,[ud_2] = N'record_count - ' + convert(varchar(max) ,isnull(@V0count ,'')) +N':' +  convert(varchar(max) ,isnull(@V1count ,N''))
      ,[ud_3] = N'delta_count - ' + convert(varchar(max) ,isnull(@delta0 ,N'')) +N':' +  convert(varchar(max) ,isnull(@delta1 ,N''))
      ,[PassFail] = @PassFail;
end

end
/*
go

declare @PassFail nchar(4);

exec [dbo].[usp_pca_validate_xp]
   @source_schema = 'pcastage' 
  ,@source_name = 'PC_PATIENTS1'
  ,@target_schema = 'dbo'
  ,@PassFail= @PassFail output;

select [@PassFail] = @PassFail;

select * from [dbo].[pca_validation_log]
*/
SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO

create or alter procedure [dbo].[usp_pca_validate_df]
   @source_schema nvarchar(128) = N'pcastage'
  ,@source_name nvarchar(128) = N'PC_PATIENTS1'
  ,@target_schema nvarchar(128) = N'dbo'
  ,@source_df_name nvarchar(128) = N'DF__PC_PATIEN__Berea__0699F50F' --N'DF_PC_PATIENTS1_insertdate' --
  ,@PassFail nchar(4) output
as
begin
  set nocount on;

  declare @now datetimeoffset = sysdatetimeoffset();
  declare @target_name varchar(128) = @source_name;
  --declare @source_df_name varchar(128) = @source_df_name;

declare 
     @source_objid int = (select [object_id] from sys.objects o join sys.schemas s on o.[schema_id] = s.[schema_id] and s.[name] = @source_schema and o.name = @source_name)
    ,@mirror_objid int = (select [object_id] from sys.objects o join sys.schemas s on o.[schema_id] = s.[schema_id] and s.[name] = @target_schema and o.name = @target_name);

declare @source_col_name varchar(128) = (
        select COL_NAME(parent_object_id , parent_column_id)
        from sys.default_constraints cc
        where cc.parent_object_id = @source_objid
        and cc.[schema_id] = schema_id(@source_schema)
        and cc.[name] = @source_df_name
        );

create table #s(fq_name varchar(max) ,property varchar(max) ,p_value varchar(max));
create table #m(fq_name varchar(max) ,property varchar(max) ,p_value varchar(max));

begin

    CREATE TABLE [#temp_df](
        [constraint_schema_name] [nvarchar](128) NULL,
        [constraint_name] [sysname] NULL,
        [parent_schema_name] [nvarchar](128) NULL,
        [parent_obj_name] [nvarchar](128) NULL,
        [parent_column_name] [nvarchar](128) NULL,
        [principal_id] [int] NULL,
        [type] [char](2) NULL,
        [is_published] [bit] NOT NULL,
        [is_schema_published] [bit] NOT NULL,
        [definition] [nvarchar](max) NULL
    );

    BEGIN
        insert into [#temp_df]
        select 
            [constraint_schema_name] = SCHEMA_NAME(cc.[schema_id])
            ,[constraint_name] = case when cc.[is_system_named] = 0 then cc.[name] else 'system_named' end
            ,[parent_schema_name] = OBJECT_SCHEMA_NAME(cc.[parent_object_id])
            ,[parent_obj_name] = object_name(parent_object_id)
            ,[parent_column_name] = COL_NAME(parent_object_id , parent_column_id)
            ,[principal_id]
            ,[type]
            ,[is_published]
            ,[is_schema_published]
            ,[definition] = case 
                when [definition] in(N'((0))' ,N'0') then N'(0)' 
                when [definition] in(N'((1))' ,N'1') then N'(1)' 
                when [definition] in(N'((2))' ,N'2') then N'(2)' 
                when [definition] in(N'((6))' ,N'6') then N'(6)' 
                when [definition] in(N'((7))' ,N'7') then N'(7)' 
                when [definition] in(N'((14))' ,N'14') then N'(14)' 
                when [definition] in(N'((60))' ,N'60') then N'(60)' 
                when [definition] in(N'((90))' ,N'90') then N'(90)' 
                when [definition] in(N'((255))' ,N'255') then N'(255)' 
                when [definition] in(N'((1.0))' ,N'1.0') then N'(1.0)' 
                when [definition] in(N'((10.00))' ,N'10.00') then N'(10.00)'
                else [definition] end
        from sys.default_constraints cc
        where cc.[is_ms_shipped] = 0 
        and cc.parent_object_id in(@source_objid ,@mirror_objid)
        and cc.[schema_id] in((schema_id(@source_schema)) ,(schema_id(@target_schema)))
        and COL_NAME(parent_object_id , parent_column_id) = @source_col_name;
    END
end

/* to force failure testing, we alter properties for target or source
if 1=0
BEGIN
    update [#temp_df]
    set [principal_id] = 1 ,[type] = null ,[is_published] = 1
    --set [principal_id] = 1 
    --set [type] = null
    --where [constraint_name] = @source_df_name
    where [constraint_name] = @source_df_name
END
*/

begin


    insert into #s
    select 
         fq_name ,property ,p_value
    from 
    (select
             [fq_name] = convert(varchar(max) ,([constraint_schema_name] +'.' +[constraint_name])) 
            ,[parent_column_name] = isnull( (CAST([parent_column_name] collate database_default AS VARCHAR(max))) ,'')
            ,[principal_id] = convert(varchar(max) ,isnull([principal_id] ,''))
            ,[type] = isnull( (CAST([type] collate database_default AS VARCHAR(max))) ,'')
            ,[is_published] = convert(varchar(max) ,isnull([is_published] ,''))
            ,[is_schema_published] = convert(varchar(max) ,isnull([is_schema_published] ,''))
            ,[definition]  = isnull( (CAST([definition] collate database_default AS VARCHAR(max))) ,'')
        from [#temp_df] cc
        where cc.[constraint_schema_name] = @source_schema
        ) c
    UNPIVOT (p_value for property in(
             [parent_column_name]
            ,[principal_id] 
            ,[type]
            ,[is_published]
            ,[is_schema_published]
            ,[definition]
            ))
     as unpvt    

    insert into #m
    select 
         fq_name ,property ,p_value
    from 
    (select
             [fq_name] = convert(varchar(max) ,([constraint_schema_name] +'.' +[constraint_name])) 
            ,[parent_column_name] = isnull( (CAST([parent_column_name] collate database_default AS VARCHAR(max))) ,'')
            ,[principal_id] = convert(varchar(max) ,isnull([principal_id] ,''))
            ,[type] = isnull( (CAST([type] collate database_default AS VARCHAR(max))) ,'')
            ,[is_published] = convert(varchar(max) ,isnull([is_published] ,''))
            ,[is_schema_published] = convert(varchar(max) ,isnull([is_schema_published] ,''))
            ,[definition]  = isnull( (CAST([definition] collate database_default AS VARCHAR(max))) ,'')
        from [#temp_df] cc
        where cc.[constraint_schema_name] = @target_schema
        ) c
    UNPIVOT (p_value for property in(
             [parent_column_name]
            ,[principal_id] 
            ,[type]
            ,[is_published]
            ,[is_schema_published]
            ,[definition]
            ))
    as unpvt    

    declare @delta_list varchar(4000);
    declare @s_ct int = (select count(*) from #s);
    declare @m_ct int = (select count(*) from #m);

    if @s_ct > 0 and @m_ct > 0 
    begin
    with cte as(
        select [property] ,[p_value] from #s
        except   
        select [property] ,[p_value] from #m
        union all
        select [property] ,[p_value] from #m
        except   
        select [property] ,[p_value] from #s
    )
        select @delta_list = STUFF((SELECT distinct ',' + (property + ':' + p_value)
        from cte 
        FOR XML PATH('')), 1, 1, '');
    end
    else
    begin
    select @delta_list = case 
        when @s_ct = 0 and @m_ct = 0 then 'source and target obj NOT exist'
        when @s_ct = 0 and @m_ct > 0 then 'source obj NOT exist'
        when @s_ct > 0 and @m_ct = 0 then 'target obj NOT exist'
        else null end
    end

    select @PassFail = case when len(@delta_list) > 0 then N'Fail' else N'Pass' end;

    begin
      insert into [dbo].[pca_validation_log]([i],[db],[ud_0],[ud_1],[ud_2],[ud_3],[PassFail])
      select 
         [i] = @@servername
        ,[db] = db_name() 
        ,[ud_0] = @now
        ,[ud_1] = N'DF'
        ,[ud_2] = concat(N'col_name: ' ,@source_col_name ,N', source_df_name: ',@source_df_name ,N' target: ' ,@target_schema ,N'.',@target_name ,N', source: ' ,@source_schema ,N'.',@source_name) 
        ,[ud_3] = @delta_list
        ,[PassFail] = @PassFail;
    end

end
end
/*
go
declare @PassFail nchar(4);

-- source_df_name: DF__CLTRATEX__CLWNS2__3CDE6358 target: tstmirrordbo.CLTRATE, source: dbo.CLTRATE	target obj NOT exist
-- source_df_name: DF_COMPANIES_company_DefaultDaysForApprovingPhysicianOrder target: tstmirrordbo.COMPANIES, source: dbo.COMPANIES	definition:((14))
--  [tstmirrordbo].[AUDIT_AUTHORIZATIONS] DROP CONSTRAINT if exists [DF_AUDIT_AUTHORIZATIONS_auth_rap_1];
-- col_name: cebh_datetransferred, source_df_name: DF_CLIENT_EPISODE_BRANCH_HISTORY_cebh_datetransferred target: tstmirrordbo.CLIENT_EPISODE_BRANCH_HISTORY, source: dbo.CLIENT_EPISODE_BRANCH_HISTORY

exec [dbo].[usp_pca_validate_df]
   @source_schema = N'dbo'
  ,@source_name  = N'CLIENT_EPISODE_BRANCH_HISTORY'
  ,@target_schema = N'tstmirrordbo'
  ,@source_df_name = N'DF_CLIENT_EPISODE_BRANCH_HISTORY_cebh_datetransferred' --N'DF_PC_PATIENTS1_insertdate' --
  ,@PassFail = @PassFail output

select [@PassFail] = @PassFail;

--select * from [dbo].[pca_validation_log]

*/
--def get_sql_column_validation(source_schema ,source_name ,target_schema ,column_name):
--    mssql_query =  f"""
/* inputs */
SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO
go
create or alter procedure [dbo].[usp_pca_validate_column_definition]
   @source_schema nvarchar(128) = N'pcastage' 
  ,@target_schema nvarchar(128) = N'dbo' 
  ,@source_name nvarchar(128) = N'PC_GPSVisitLocations' 
  ,@source_column_name nvarchar(128) = N'latitude'
  ,@PassFail nchar(4) output
as
begin
  set nocount on;

  declare @now datetimeoffset = sysdatetimeoffset();
  
  declare @target_name nvarchar(128) = @source_name;

  declare 
     @source_objid int = (select [object_id] from sys.tables o join sys.schemas s on o.[schema_id] = s.[schema_id] and s.[name] = @source_schema and o.name = @source_name)
    ,@target_objid int = (select [object_id] from sys.tables o join sys.schemas s on o.[schema_id] = s.[schema_id] and s.[name] = @target_schema and o.name = @target_name);

create table #s(fq_name varchar(max) ,property varchar(max) ,p_value varchar(max));
create table #m(fq_name varchar(max) ,property varchar(max) ,p_value varchar(max));
    
SELECT 
       [table_schema_name] = object_schema_name(c.[object_id]) 
      ,[table_name]  = object_name(c.[object_id]) 
      ,c.[name]
      ,c.[system_type_id]
      ,c.[user_type_id]
      ,c.[max_length]
      ,c.[precision]
      ,c.[scale]
      ,c.[collation_name]
      ,c.[is_nullable]
--      ,c.[is_ansi_padded]
      ,c.[is_rowguidcol]
      ,c.[is_identity]
      ,c.[is_computed]
      ,cc.[is_persisted]
      ,c.[is_filestream]
      ,c.[is_replicated]
      ,c.[is_non_sql_subscribed]
      ,c.[is_merge_published]
      ,c.[is_dts_replicated]
      ,c.[is_xml_document]
      ,c.[xml_collection_id]      
      ,[default_object_name] = case when dc.[is_system_named] = 1 then 'system_named' else object_name(c.default_object_id) end
      ,c.[rule_object_id]
      ,c.[is_sparse]
      ,c.[is_column_set]
      ,c.[generated_always_type]
      ,c.[encryption_type]
      ,c.[encryption_algorithm_name]
      ,c.[column_encryption_key_id]
      ,c.[column_encryption_key_database_name]
      ,c.[is_hidden]
      ,c.[is_masked]
      ,c.[graph_type]
into #temp_c
  from [sys].[columns] c
  left join sys.computed_columns cc on c.[object_id] = cc.[object_id] and c.[column_id] = cc.[column_id] and c.[is_computed] = 1 and cc.[is_persisted] = 1
  left join sys.default_constraints dc on dc.[object_id] = c.default_object_id
  where c.[object_id] in( @source_objid ,@target_objid)
  and c.[name] = @source_column_name;

/* to force failure testing, we alter properties for target or source
if 1=0
BEGIN
    update [#temp_c]
    set [collation_name] = 17 ,[user_type_id] = 17 ,[is_nullable] = 0
    where [table_name] = @source_name
END
*/

begin
        insert into #s
        select 
            fq_name ,property ,p_value
        from 
        (select
                [fq_name] = convert(varchar(max) ,([table_schema_name] +'.'+ [table_name] +'.'+[name])) 
                ,[system_type_id] = convert(varchar(max) ,isnull([system_type_id] ,''))
                ,[user_type_id] = convert(varchar(max) ,isnull([user_type_id] ,''))
                ,[max_length] = convert(varchar(max) ,isnull([max_length] ,''))
                ,[precision] = convert(varchar(max) ,isnull([precision] ,''))
                ,[scale] = convert(varchar(max) ,isnull([scale] ,''))
                ,[collation_name] = isnull( (CAST([collation_name] collate database_default AS VARCHAR(max))) ,'')
                ,[is_nullable] = convert(varchar(max) ,isnull([is_nullable] ,''))
                --,[is_ansi_padded] = convert(varchar(max) ,isnull([is_ansi_padded] ,''))
                ,[is_rowguidcol] = convert(varchar(max) ,isnull([is_rowguidcol] ,''))
                ,[is_identity] = convert(varchar(max) ,isnull([is_identity] ,''))
                ,[is_computed] = convert(varchar(max) ,isnull([is_computed] ,''))
                ,[is_persisted] = convert(varchar(max) ,isnull([is_persisted] ,''))
                ,[is_filestream] = convert(varchar(max) ,isnull([is_filestream] ,''))
                ,[is_replicated] = convert(varchar(max) ,isnull([is_replicated] ,''))
                ,[is_non_sql_subscribed] = convert(varchar(max) ,isnull([is_non_sql_subscribed] ,''))
                ,[is_merge_published] = convert(varchar(max) ,isnull([is_merge_published] ,''))
                ,[is_dts_replicated] = convert(varchar(max) ,isnull([is_dts_replicated] ,''))
                ,[is_xml_document] = convert(varchar(max) ,isnull([is_xml_document] ,''))
                ,[xml_collection_id] = convert(varchar(max) ,isnull([xml_collection_id] ,''))
                ,[default_object_name] = isnull( (CAST([default_object_name] collate database_default AS VARCHAR(max))) ,'')
                ,[rule_object_id] = convert(varchar(max) ,isnull([rule_object_id] ,''))
                ,[is_sparse] = convert(varchar(max) ,isnull([is_sparse] ,''))
                ,[is_column_set] = convert(varchar(max) ,isnull([is_column_set] ,''))
                ,[generated_always_type] = convert(varchar(max) ,isnull([generated_always_type] ,''))
                ,[encryption_type] = convert(varchar(max) ,isnull([encryption_type] ,''))
                ,[encryption_algorithm_name] = isnull( (CAST([encryption_algorithm_name] collate database_default AS VARCHAR(max))) ,'')
                ,[column_encryption_key_id] = convert(varchar(max) ,isnull([column_encryption_key_id] ,''))
                ,[column_encryption_key_database_name] = isnull( (CAST([column_encryption_key_database_name] collate database_default AS VARCHAR(max))) ,'')
                ,[is_hidden] = convert(varchar(max) ,isnull([is_hidden] ,''))
                ,[is_masked] = convert(varchar(max) ,isnull([is_masked] ,''))
                ,[graph_type] = convert(varchar(max) ,isnull([graph_type] ,''))
            from [#temp_c] cc
            where cc.[table_schema_name] = @source_schema
            and cc.[table_name] = @source_name
            ) c
        UNPIVOT (p_value for property in(
                 [system_type_id]
                ,[user_type_id]
                ,[max_length]
                ,[precision]
                ,[scale]
                ,[collation_name]
                ,[is_nullable]
                --,[is_ansi_padded]
                ,[is_rowguidcol]
                ,[is_identity]
                ,[is_computed]
                ,[is_persisted]
                ,[is_filestream]
                ,[is_replicated]
                ,[is_non_sql_subscribed] 
                ,[is_merge_published] 
                ,[is_dts_replicated] 
                ,[is_xml_document] 
                ,[xml_collection_id] 
                ,[default_object_name] 
                ,[rule_object_id]
                ,[is_sparse]
                ,[is_column_set] 
                ,[generated_always_type]
                ,[encryption_type] 
                ,[encryption_algorithm_name]
                ,[column_encryption_key_id]
                ,[column_encryption_key_database_name]
                ,[is_hidden]
                ,[is_masked]
                ,[graph_type]
                ))
        as unpvt;    

        insert into #m
        select 
            fq_name ,property ,p_value
        from 
        (select
                [fq_name] = convert(varchar(max) ,([table_schema_name] +'.'+ [table_name] +'.'+[name])) 
                ,[system_type_id] = convert(varchar(max) ,isnull([system_type_id] ,''))
                ,[user_type_id] = convert(varchar(max) ,isnull([user_type_id] ,''))
                ,[max_length] = convert(varchar(max) ,isnull([max_length] ,''))
                ,[precision] = convert(varchar(max) ,isnull([precision] ,''))
                ,[scale] = convert(varchar(max) ,isnull([scale] ,''))
                ,[collation_name] = isnull( (CAST([collation_name] collate database_default AS VARCHAR(max))) ,'')
                ,[is_nullable] = convert(varchar(max) ,isnull([is_nullable] ,''))
                --,[is_ansi_padded] = convert(varchar(max) ,isnull([is_ansi_padded] ,''))
                ,[is_rowguidcol] = convert(varchar(max) ,isnull([is_rowguidcol] ,''))
                ,[is_identity] = convert(varchar(max) ,isnull([is_identity] ,''))
                ,[is_computed] = convert(varchar(max) ,isnull([is_computed] ,''))
                ,[is_persisted] = convert(varchar(max) ,isnull([is_persisted] ,''))
                ,[is_filestream] = convert(varchar(max) ,isnull([is_filestream] ,''))
                ,[is_replicated] = convert(varchar(max) ,isnull([is_replicated] ,''))
                ,[is_non_sql_subscribed] = convert(varchar(max) ,isnull([is_non_sql_subscribed] ,''))
                ,[is_merge_published] = convert(varchar(max) ,isnull([is_merge_published] ,''))
                ,[is_dts_replicated] = convert(varchar(max) ,isnull([is_dts_replicated] ,''))
                ,[is_xml_document] = convert(varchar(max) ,isnull([is_xml_document] ,''))
                ,[xml_collection_id] = convert(varchar(max) ,isnull([xml_collection_id] ,''))
                ,[default_object_name] = isnull( (CAST([default_object_name] collate database_default AS VARCHAR(max))) ,'')
                ,[rule_object_id] = convert(varchar(max) ,isnull([rule_object_id] ,''))
                ,[is_sparse] = convert(varchar(max) ,isnull([is_sparse] ,''))
                ,[is_column_set] = convert(varchar(max) ,isnull([is_column_set] ,''))
                ,[generated_always_type] = convert(varchar(max) ,isnull([generated_always_type] ,''))
                ,[encryption_type] = convert(varchar(max) ,isnull([encryption_type] ,''))
                ,[encryption_algorithm_name] = isnull( (CAST([encryption_algorithm_name] collate database_default AS VARCHAR(max))) ,'')
                ,[column_encryption_key_id] = convert(varchar(max) ,isnull([column_encryption_key_id] ,''))
                ,[column_encryption_key_database_name] = isnull( (CAST([column_encryption_key_database_name] collate database_default AS VARCHAR(max))) ,'')
                ,[is_hidden] = convert(varchar(max) ,isnull([is_hidden] ,''))
                ,[is_masked] = convert(varchar(max) ,isnull([is_masked] ,''))
                ,[graph_type] = convert(varchar(max) ,isnull([graph_type] ,''))
            from [#temp_c] cc
            where cc.[table_schema_name] = @target_schema
            and cc.[table_name] = @target_name
            ) c
        UNPIVOT (p_value for property in(
                 [system_type_id]
                ,[user_type_id]
                ,[max_length]
                ,[precision]
                ,[scale]
                ,[collation_name]
                ,[is_nullable]
                --,[is_ansi_padded]
                ,[is_rowguidcol]
                ,[is_identity]
                ,[is_computed]
                ,[is_persisted]
                ,[is_filestream]
                ,[is_replicated]
                ,[is_non_sql_subscribed] 
                ,[is_merge_published] 
                ,[is_dts_replicated] 
                ,[is_xml_document] 
                ,[xml_collection_id] 
                ,[default_object_name] 
                ,[rule_object_id]
                ,[is_sparse]
                ,[is_column_set] 
                ,[generated_always_type]
                ,[encryption_type] 
                ,[encryption_algorithm_name]
                ,[column_encryption_key_id]
                ,[column_encryption_key_database_name]
                ,[is_hidden]
                ,[is_masked]
                ,[graph_type]
                ))
        as unpvt; 
END

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

if @delta_list is null
begin
  set @PassFail = N'Pass';
end
else
begin
  set @PassFail = N'Fail';
end

begin
  insert into [dbo].[pca_validation_log]([i],[db],[ud_0],[ud_1],[ud_2],[ud_3],[PassFail])
  select 
     [i] = @@servername
    ,[db] = db_name() 
    ,[ud_0] = @now
    ,[ud_1] = N'table_column_definition'
    ,[ud_2] = concat(N'source_column_name: ',@source_column_name ,N' target: ' ,@target_schema ,N'.',@target_name ,N', source: ' ,@source_schema ,N'.',@source_name) 
    ,[ud_3] = @delta_list
    ,[PassFail] = @PassFail;
end

end
--"""
--    return mssql_query
SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO
create or alter procedure [dbo].[usp_pca_validate_table_definition]
   @source_schema nvarchar(128)
  ,@source_name nvarchar(128)
  ,@target_schema nvarchar(128)
  ,@PassFail nchar(4) output
as
begin
set nocount on;

declare @now datetimeoffset = sysdatetimeoffset();
declare @target_name varchar(128) = @source_name;

declare 
     @source_objid int = (select [object_id] from sys.tables o join sys.schemas s on o.[schema_id] = s.[schema_id] and s.[name] = @source_schema and o.name = @source_name)
    ,@target_objid int = (select [object_id] from sys.tables o join sys.schemas s on o.[schema_id] = s.[schema_id] and s.[name] = @target_schema and o.name = @target_name);

create table #s(fq_name varchar(max) ,property varchar(max) ,p_value varchar(max));
create table #m(fq_name varchar(max) ,property varchar(max) ,p_value varchar(max));

select 
     [schemaname] = s.name 
    ,o.[name]
    ,o.[principal_id]
    ,[parent_object_id]
    ,[type]
    ,[is_ms_shipped]
    ,[is_published]
    ,[is_schema_published]
    --,[lob_data_space_id]
    --,[filestream_data_space_id]
    ,[lock_on_bulk_load]
    ,[uses_ansi_nulls]
    ,[is_replicated]
    ,[has_replication_filter]
    ,[is_merge_published]
    ,[is_sync_tran_subscribed]
    ,[has_unchecked_assembly_data]
    ,[text_in_row_limit]
    ,[large_value_types_out_of_row]
    ,[is_tracked_by_cdc]
    ,[lock_escalation]
    ,[lock_escalation_desc]
    ,[is_filetable]
    ,[is_memory_optimized]
    ,[durability]
    ,[durability_desc]
    ,[temporal_type]
    ,[temporal_type_desc]
    ,[history_table_id]
    ,[is_remote_data_archive_enabled]
    ,[is_external]
    ,[history_retention_period]
    ,[history_retention_period_unit]
    ,[history_retention_period_unit_desc]
    ,[is_node]
    ,[is_edge]
into [#temp_t]
FROM [sys].[tables] o
    join sys.schemas s on o.[schema_id] = s.[schema_id] 
        and o.[object_id] in( @source_objid ,@target_objid)

/* to force failure testing, we alter properties for target or source
if 1=0
BEGIN
    update [#temp_t]
    set [principal_id] = 1 ,[type] = null ,[parent_object_id] = 1
    --set [principal_id] = 1 
    --set [type] = null
    --where [name] = @source_name
    where [name] = @target_name
END
*/

begin
    insert into #s
    select 
        fq_name ,property ,p_value
    from 
    (select
            [fq_name] = convert(varchar(max) ,([schemaname] +'.'+ [name])) 
            ,[principal_id] = convert(varchar(max) ,isnull([principal_id] ,''))
            ,[parent_object_id] = convert(varchar(max) ,isnull([parent_object_id] ,''))
            ,[type] = isnull( (CAST([type] collate database_default AS VARCHAR(max))) ,'')
            ,[is_ms_shipped] = convert(varchar(max) ,isnull([is_ms_shipped] ,''))
            ,[is_published] = convert(varchar(max) ,isnull([is_published] ,''))
            ,[is_schema_published] = convert(varchar(max) ,isnull([is_schema_published] ,''))
            --,[lob_data_space_id] = convert(varchar(max) ,isnull([lob_data_space_id] ,''))
            --,[filestream_data_space_id] = convert(varchar(max) ,isnull([filestream_data_space_id] ,''))
            ,[lock_on_bulk_load] = convert(varchar(max) ,isnull([lock_on_bulk_load] ,''))
            ,[uses_ansi_nulls] = convert(varchar(max) ,isnull([uses_ansi_nulls] ,''))
            ,[is_replicated] = convert(varchar(max) ,isnull([is_replicated] ,''))
            ,[has_replication_filter] = convert(varchar(max) ,isnull([has_replication_filter] ,''))
            ,[is_merge_published] = convert(varchar(max) ,isnull([is_merge_published] ,''))
            ,[is_sync_tran_subscribed] = convert(varchar(max) ,isnull([is_sync_tran_subscribed] ,''))
            ,[has_unchecked_assembly_data] = convert(varchar(max) ,isnull([has_unchecked_assembly_data] ,''))
            ,[text_in_row_limit] = convert(varchar(max) ,isnull([text_in_row_limit] ,''))
            ,[large_value_types_out_of_row] = convert(varchar(max) ,isnull([large_value_types_out_of_row] ,''))
            ,[is_tracked_by_cdc] = convert(varchar(max) ,isnull([is_tracked_by_cdc] ,''))
            ,[lock_escalation] = convert(varchar(max) ,isnull([lock_escalation] ,''))
            ,[is_filetable] = convert(varchar(max) ,isnull([is_filetable] ,''))
            ,[is_memory_optimized] = convert(varchar(max) ,isnull([is_memory_optimized] ,''))
            ,[durability] = convert(varchar(max) ,isnull([durability] ,''))
            ,[temporal_type] = convert(varchar(max) ,isnull([temporal_type] ,''))
            ,[history_table_id] = convert(varchar(max) ,isnull([history_table_id] ,''))
            ,[is_remote_data_archive_enabled] = convert(varchar(max) ,isnull([is_remote_data_archive_enabled] ,''))
            ,[is_external] = convert(varchar(max) ,isnull([is_external] ,''))
            ,[history_retention_period] = convert(varchar(max) ,isnull([history_retention_period] ,''))
            ,[history_retention_period_unit] = convert(varchar(max) ,isnull([history_retention_period_unit] ,''))
            ,[is_node] = convert(varchar(max) ,isnull([is_node] ,''))
            ,[is_edge] = convert(varchar(max) ,isnull([is_edge] ,''))
        from [#temp_t] cc
        where cc.[schemaname] = @source_schema
        and cc.[name] = @source_name
        ) c
    UNPIVOT (p_value for property in(
            [principal_id]
            ,[parent_object_id]
            ,[type]
            ,[is_ms_shipped]
            ,[is_published] 
            ,[is_schema_published] 
            --,[lob_data_space_id] 
            --,[filestream_data_space_id] 
            ,[lock_on_bulk_load] 
            ,[uses_ansi_nulls] 
            ,[is_replicated] 
            ,[has_replication_filter]
            ,[is_merge_published] 
            ,[is_sync_tran_subscribed]
            ,[has_unchecked_assembly_data] 
            ,[text_in_row_limit] 
            ,[large_value_types_out_of_row] 
            ,[is_tracked_by_cdc] 
            ,[lock_escalation] 
            ,[is_filetable] 
            ,[is_memory_optimized] 
            ,[durability] 
            ,[temporal_type] 
            ,[history_table_id]
            ,[is_remote_data_archive_enabled]
            ,[is_external] 
            ,[history_retention_period] 
            ,[history_retention_period_unit]
            ,[is_node]
            ,[is_edge]
            ))
    as unpvt;    

    insert into #m
    select 
        fq_name ,property ,p_value
    from 
    (select
            [fq_name] = convert(varchar(max) ,([schemaname] +'.'+ [name])) 
            ,[principal_id] = convert(varchar(max) ,isnull([principal_id] ,''))
            ,[parent_object_id] = convert(varchar(max) ,isnull([parent_object_id] ,''))
            ,[type] = isnull( (CAST([type] collate database_default AS VARCHAR(max))) ,'')
            ,[is_ms_shipped] = convert(varchar(max) ,isnull([is_ms_shipped] ,''))
            ,[is_published] = convert(varchar(max) ,isnull([is_published] ,''))
            ,[is_schema_published] = convert(varchar(max) ,isnull([is_schema_published] ,''))
            --,[lob_data_space_id] = convert(varchar(max) ,isnull([lob_data_space_id] ,''))
            --,[filestream_data_space_id] = convert(varchar(max) ,isnull([filestream_data_space_id] ,''))
            ,[lock_on_bulk_load] = convert(varchar(max) ,isnull([lock_on_bulk_load] ,''))
            ,[uses_ansi_nulls] = convert(varchar(max) ,isnull([uses_ansi_nulls] ,''))
            ,[is_replicated] = convert(varchar(max) ,isnull([is_replicated] ,''))
            ,[has_replication_filter] = convert(varchar(max) ,isnull([has_replication_filter] ,''))
            ,[is_merge_published] = convert(varchar(max) ,isnull([is_merge_published] ,''))
            ,[is_sync_tran_subscribed] = convert(varchar(max) ,isnull([is_sync_tran_subscribed] ,''))
            ,[has_unchecked_assembly_data] = convert(varchar(max) ,isnull([has_unchecked_assembly_data] ,''))
            ,[text_in_row_limit] = convert(varchar(max) ,isnull([text_in_row_limit] ,''))
            ,[large_value_types_out_of_row] = convert(varchar(max) ,isnull([large_value_types_out_of_row] ,''))
            ,[is_tracked_by_cdc] = convert(varchar(max) ,isnull([is_tracked_by_cdc] ,''))
            ,[lock_escalation] = convert(varchar(max) ,isnull([lock_escalation] ,''))
            ,[is_filetable] = convert(varchar(max) ,isnull([is_filetable] ,''))
            ,[is_memory_optimized] = convert(varchar(max) ,isnull([is_memory_optimized] ,''))
            ,[durability] = convert(varchar(max) ,isnull([durability] ,''))
            ,[temporal_type] = convert(varchar(max) ,isnull([temporal_type] ,''))
            ,[history_table_id] = convert(varchar(max) ,isnull([history_table_id] ,''))
            ,[is_remote_data_archive_enabled] = convert(varchar(max) ,isnull([is_remote_data_archive_enabled] ,''))
            ,[is_external] = convert(varchar(max) ,isnull([is_external] ,''))
            ,[history_retention_period] = convert(varchar(max) ,isnull([history_retention_period] ,''))
            ,[history_retention_period_unit] = convert(varchar(max) ,isnull([history_retention_period_unit] ,''))
            ,[is_node] = convert(varchar(max) ,isnull([is_node] ,''))
            ,[is_edge] = convert(varchar(max) ,isnull([is_edge] ,''))
        from [#temp_t] cc
        where cc.[schemaname] = @target_schema
        and cc.[name] = @target_name
        ) c
    UNPIVOT (p_value for property in(
        [principal_id]
        ,[parent_object_id]
        ,[type]
        ,[is_ms_shipped]
        ,[is_published]
        ,[is_schema_published]
        --,[lob_data_space_id]
        --,[filestream_data_space_id]
        ,[lock_on_bulk_load]
        ,[uses_ansi_nulls]
        ,[is_replicated]
        ,[has_replication_filter]
        ,[is_merge_published]
        ,[is_sync_tran_subscribed]
        ,[has_unchecked_assembly_data]
        ,[text_in_row_limit]
        ,[large_value_types_out_of_row]
        ,[is_tracked_by_cdc]
        ,[lock_escalation]
        ,[is_filetable]
        ,[is_memory_optimized]
        ,[durability]
        ,[temporal_type]
        ,[history_table_id]
        ,[is_remote_data_archive_enabled]
        ,[is_external]
        ,[history_retention_period]
        ,[history_retention_period_unit]
        ,[is_node]
        ,[is_edge]
            ))
    as unpvt; 

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
    ,[ud_1] = N'table_definition'
    ,[ud_2] = concat(N'target: ' ,@target_schema ,N'.',@target_name ,N', source: ' ,@source_schema ,N'.',@source_name) 
    ,[ud_3] = @delta_list
    ,[PassFail] = @PassFail;
end

END
end
/*
go
declare @PassFail nchar(4);

exec [dbo].[usp_pca_validate_table_definition]
   @source_schema = N'pcastage' 
  ,@source_name = N'PC_PATIENTS1'
  ,@target_schema = N'dbo'
  ,@PassFail= @PassFail output;

select [@PassFail] = @PassFail;

--select * from [dbo].[pca_validation_log]
*/
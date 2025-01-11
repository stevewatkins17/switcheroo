SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO
create or alter procedure [dbo].[usp_pca_validate_ix]
   @source_schema nvarchar(128) 
  ,@source_name nvarchar(128)
  ,@target_schema nvarchar(128)
  ,@source_idx_name nvarchar(128)
  ,@PassFail nchar(4) output
as
begin
set nocount on;

declare @now datetimeoffset = sysdatetimeoffset();
declare @target_name varchar(128) = @source_name;
declare @target_idx_name varchar(128) = @source_idx_name;

declare 
     @source_objid int = (select [object_id] from sys.objects o join sys.schemas s on o.[schema_id] = s.[schema_id] and s.[name] = @source_schema and o.name = @source_name)
    ,@target_objid int = (select [object_id] from sys.objects o join sys.schemas s on o.[schema_id] = s.[schema_id] and s.[name] = @target_schema and o.name = @target_name);

create table #s(fq_name varchar(max) ,property varchar(max) ,p_value varchar(max));
create table #m(fq_name varchar(max) ,property varchar(max) ,p_value varchar(max));

/* index properties */ 
SELECT 
     [object_id]
    ,[name]
    ,[index_id]
    ,[type]
    ,[type_desc]
    ,[is_unique]
    --,[data_space_id]
    ,[ignore_dup_key]
    ,[is_primary_key]
    ,[is_unique_constraint]
    ,[fill_factor]
    ,[is_padded]
    ,[is_disabled]
    ,[is_hypothetical]
    ,[is_ignored_in_optimization]
    ,[allow_row_locks]
    ,[allow_page_locks]
    ,[has_filter]
    ,[filter_definition]
    ,[compression_delay]
    ,[suppress_dup_key_messages]
    ,[auto_created]
into #temp_indexes 
FROM [sys].[indexes]
where [object_id] in(@source_objid ,@target_objid)
and [name] in(@source_idx_name ,@target_idx_name);

/*  get strings used to assemble the create index stmt 
    previously - "[create] rowstore index build strings.sql"*/
declare 
     @string_separator varchar(10) = ' ,'
    ,@key_string varchar(max)
    ,@include_string varchar(max)
    ,@filter_dfn varchar(max)
    ,@IndexOptions varchar(max);

declare @index_columns TABLE(
    [index_name] [sysname] NULL,
    [index_column_id] [int] NOT NULL,
    [key_ordinal] [tinyint] NOT NULL,
    [is_descending_key] [bit] NULL,
    [column_name] [sysname] NULL,
    [column_type] [nvarchar](128) NULL,
    [is_included_column] [bit] NULL,
    [filter_definition] [nvarchar](max) NULL,
    [is_identity] [bit] NOT NULL
);

declare 
     @source_key_string varchar(max)
    ,@source_include_string varchar(max)
    ,@source_filter_dfn varchar(max)
    ,@source_IndexOptions varchar(max)
    ,@target_key_string varchar(max) 
    ,@target_include_string varchar(max)
    ,@target_filter_dfn varchar(max)
    ,@target_IndexOptions varchar(max);

begin /* SOURCE */ 
    insert into @index_columns
        ([index_name] ,[index_column_id] ,[key_ordinal] ,[is_descending_key] ,[column_name],[column_type],[is_included_column],[filter_definition],[is_identity])
    SELECT 
         [index_name] = [i].[name]
        ,[ic].[index_column_id]  
        ,[ic].[key_ordinal] 
        ,[ic].[is_descending_key] 
        ,[column_name] = [c].name
        ,[column_type] = TYPE_NAME([c].[user_type_id])
        ,[ic].[is_included_column]
        ,[i].[filter_definition] 
        ,[c].[is_identity]
    FROM sys.indexes AS i  
    INNER JOIN sys.index_columns AS ic   
        ON i.object_id = ic.object_id AND i.index_id = ic.index_id  
        AND i.object_id = @source_objid
        and i.name = @source_idx_name  
    INNER JOIN sys.columns AS c   
        ON ic.object_id = c.object_id AND c.column_id = ic.column_id;      

    select @source_IndexOptions = 
	   CASE WHEN [ix].[is_padded] = 1 THEN 'PAD_INDEX = ON, '
			  ELSE 'PAD_INDEX = OFF, '
		  END + CASE WHEN [ix].[allow_page_locks] = 1 THEN 'ALLOW_PAGE_LOCKS = ON, '
				  ELSE 'ALLOW_PAGE_LOCKS = OFF, '
				  END + CASE WHEN [ix].[allow_row_locks] = 1 THEN 'ALLOW_ROW_LOCKS = ON, '
						  ELSE 'ALLOW_ROW_LOCKS = OFF, '
					  END + CASE WHEN INDEXPROPERTY(@source_objid, [ix].[name], 'IsStatistics') = 1 THEN 'STATISTICS_NORECOMPUTE = ON, '
							  ELSE 'STATISTICS_NORECOMPUTE = OFF, '
							  END + CASE WHEN [ix].[ignore_dup_key] = 1 THEN 'IGNORE_DUP_KEY = ON, '
									  ELSE 'IGNORE_DUP_KEY = OFF, '
								  END + 'SORT_IN_TEMPDB = OFF ' + CASE WHEN [ix].[fill_factor] = 0 THEN '' ELSE ',FILLFACTOR =' + CAST([ix].[fill_factor] AS VARCHAR(3)) END

    from sys.indexes AS ix
    where ix.object_id = @source_objid
        and ix.name = @source_idx_name;

    select @source_key_string = (coalesce(@key_string + @string_separator ,'') + quotename([column_name]) + space(1) + (case when [is_descending_key] = 0 then 'asc' else 'desc' end) )
    from @index_columns ic
    where ic.key_ordinal > 0
    order by ic.key_ordinal;

    select @source_include_string = (coalesce(@include_string + @string_separator ,'') + quotename([column_name]) )
    from @index_columns ic
    where ic.key_ordinal = 0
    order by ic.index_column_id;

    select top 1 @source_filter_dfn = [filter_definition] from @index_columns;
    
end

begin /* TARGET */ 
    insert into @index_columns
        ([index_name] ,[index_column_id] ,[key_ordinal] ,[is_descending_key] ,[column_name],[column_type],[is_included_column],[filter_definition],[is_identity])
    SELECT 
         [index_name] = [i].[name]
        ,[ic].[index_column_id]  
        ,[ic].[key_ordinal] 
        ,[ic].[is_descending_key] 
        ,[column_name] = [c].name
        ,[column_type] = TYPE_NAME([c].[user_type_id])
        ,[ic].[is_included_column]
        ,[i].[filter_definition] 
        ,[c].[is_identity]
    FROM sys.indexes AS i  
    INNER JOIN sys.index_columns AS ic   
        ON i.object_id = ic.object_id AND i.index_id = ic.index_id  
        AND i.object_id = @target_objid
        and i.name = @target_idx_name  
    INNER JOIN sys.columns AS c   
        ON ic.object_id = c.object_id AND c.column_id = ic.column_id;      

    select @target_IndexOptions = 
	   CASE WHEN [ix].[is_padded] = 1 THEN 'PAD_INDEX = ON, '
			  ELSE 'PAD_INDEX = OFF, '
		  END + CASE WHEN [ix].[allow_page_locks] = 1 THEN 'ALLOW_PAGE_LOCKS = ON, '
				  ELSE 'ALLOW_PAGE_LOCKS = OFF, '
				  END + CASE WHEN [ix].[allow_row_locks] = 1 THEN 'ALLOW_ROW_LOCKS = ON, '
						  ELSE 'ALLOW_ROW_LOCKS = OFF, '
					  END + CASE WHEN INDEXPROPERTY(@target_objid, [ix].[name], 'IsStatistics') = 1 THEN 'STATISTICS_NORECOMPUTE = ON, '
							  ELSE 'STATISTICS_NORECOMPUTE = OFF, '
							  END + CASE WHEN [ix].[ignore_dup_key] = 1 THEN 'IGNORE_DUP_KEY = ON, '
									  ELSE 'IGNORE_DUP_KEY = OFF, '
								  END + 'SORT_IN_TEMPDB = OFF ' + CASE WHEN [ix].[fill_factor] = 0 THEN '' ELSE ',FILLFACTOR =' + CAST([ix].[fill_factor] AS VARCHAR(3)) END

    from sys.indexes AS ix
    where ix.object_id = @target_objid
        and ix.name = @target_idx_name;

    select @target_key_string = (coalesce(@key_string + @string_separator ,'') + quotename([column_name]) + space(1) + (case when [is_descending_key] = 0 then 'asc' else 'desc' end) )
    from @index_columns ic
    where ic.key_ordinal > 0
    order by ic.key_ordinal;

    select @target_include_string = (coalesce(@include_string + @string_separator ,'') + quotename([column_name]) )
    from @index_columns ic
    where ic.key_ordinal = 0
    order by ic.index_column_id;

    select top 1 @target_filter_dfn = [filter_definition] from @index_columns;
    
end
/* to force failure testing, we alter properties for target or source
if 1=0
BEGIN
    update [#temp_indexes]
    set [fill_factor] = 10 ,[type] = 17 
    where [name] = @target_idx_name and [object_id] = @target_objid
END
*/

begin
    insert into #s
    select 
        fq_name ,property ,p_value
    from 
    (select
             [fq_name] = convert(varchar(max) ,(object_schema_name([object_id]) +'.'+ object_name([object_id]) +'.'+[name])) 
            ,[name] = isnull( (CAST([name] collate database_default AS VARCHAR(max))) ,'')
            ,[type] = convert(varchar(max) ,isnull([type] ,''))
            ,[is_unique] = convert(varchar(max) ,isnull([is_unique] ,''))
            --,[data_space_id] = convert(varchar(max) ,isnull([data_space_id] ,''))
            ,[ignore_dup_key] = convert(varchar(max) ,isnull([ignore_dup_key] ,''))
            ,[is_primary_key] = convert(varchar(max) ,isnull([is_primary_key] ,''))
            ,[is_unique_constraint] = convert(varchar(max) ,isnull([is_unique_constraint] ,''))
            ,[fill_factor] = convert(varchar(max) ,isnull([fill_factor] ,''))
            ,[is_padded] = convert(varchar(max) ,isnull([is_padded] ,''))
            ,[is_disabled] = convert(varchar(max) ,isnull([is_disabled] ,''))
            ,[is_hypothetical] = convert(varchar(max) ,isnull([is_hypothetical] ,''))
            ,[is_ignored_in_optimization] = convert(varchar(max) ,isnull([is_ignored_in_optimization] ,''))
            ,[allow_row_locks] = convert(varchar(max) ,isnull([allow_row_locks] ,''))
            ,[allow_page_locks] = convert(varchar(max) ,isnull([allow_page_locks] ,''))
            ,[has_filter] = convert(varchar(max) ,isnull([has_filter] ,''))
            ,[compression_delay] = convert(varchar(max) ,isnull([compression_delay] ,''))
            ,[suppress_dup_key_messages] = convert(varchar(max) ,isnull([suppress_dup_key_messages] ,''))
            ,[auto_created] = convert(varchar(max) ,isnull([auto_created] ,''))
            ,[key_string] = isnull( (CAST(@source_key_string collate database_default AS VARCHAR(max))) ,'')
            ,[include_string] = isnull( (CAST(@source_include_string collate database_default AS VARCHAR(max))) ,'')
            ,[filter_dfn] = isnull( (CAST(@source_filter_dfn collate database_default AS VARCHAR(max))) ,'')
            ,[IndexOptions] = isnull( (CAST(@source_IndexOptions collate database_default AS VARCHAR(max))) ,'')
        from [#temp_indexes] cc
        where [object_id] in(@source_objid)
        and [name] in(@source_idx_name)
        ) c
    UNPIVOT (p_value for property in(
             [name]
            ,[type]
            ,[is_unique] 
            --,[data_space_id] 
            ,[ignore_dup_key]
            ,[is_primary_key]
            ,[is_unique_constraint]
            ,[fill_factor]
            ,[is_padded]
            ,[is_disabled]
            ,[is_hypothetical] 
            ,[is_ignored_in_optimization]
            ,[allow_row_locks] 
            ,[allow_page_locks] 
            ,[has_filter] 
            ,[compression_delay] 
            ,[suppress_dup_key_messages]
            ,[auto_created]
            ,[key_string]
            ,[include_string]
            ,[filter_dfn]
            ,[IndexOptions]
            ))
    as unpvt;    

    insert into #m
    select 
        fq_name ,property ,p_value
    from 
    (select
             [fq_name] = convert(varchar(max) ,(object_schema_name([object_id]) +'.'+ object_name([object_id]) +'.'+[name])) 
            ,[name] = isnull( (CAST([name] collate database_default AS VARCHAR(max))) ,'')
            ,[type] = convert(varchar(max) ,isnull([type] ,''))
            ,[is_unique] = convert(varchar(max) ,isnull([is_unique] ,''))
           -- ,[data_space_id] = convert(varchar(max) ,isnull([data_space_id] ,''))
            ,[ignore_dup_key] = convert(varchar(max) ,isnull([ignore_dup_key] ,''))
            ,[is_primary_key] = convert(varchar(max) ,isnull([is_primary_key] ,''))
            ,[is_unique_constraint] = convert(varchar(max) ,isnull([is_unique_constraint] ,''))
            ,[fill_factor] = convert(varchar(max) ,isnull([fill_factor] ,''))
            ,[is_padded] = convert(varchar(max) ,isnull([is_padded] ,''))
            ,[is_disabled] = convert(varchar(max) ,isnull([is_disabled] ,''))
            ,[is_hypothetical] = convert(varchar(max) ,isnull([is_hypothetical] ,''))
            ,[is_ignored_in_optimization] = convert(varchar(max) ,isnull([is_ignored_in_optimization] ,''))
            ,[allow_row_locks] = convert(varchar(max) ,isnull([allow_row_locks] ,''))
            ,[allow_page_locks] = convert(varchar(max) ,isnull([allow_page_locks] ,''))
            ,[has_filter] = convert(varchar(max) ,isnull([has_filter] ,''))
            ,[compression_delay] = convert(varchar(max) ,isnull([compression_delay] ,''))
            ,[suppress_dup_key_messages] = convert(varchar(max) ,isnull([suppress_dup_key_messages] ,''))
            ,[auto_created] = convert(varchar(max) ,isnull([auto_created] ,''))
            ,[key_string] = isnull( (CAST(@target_key_string collate database_default AS VARCHAR(max))) ,'')
            ,[include_string] = isnull( (CAST(@target_include_string collate database_default AS VARCHAR(max))) ,'')
            ,[filter_dfn] = isnull( (CAST(@target_filter_dfn collate database_default AS VARCHAR(max))) ,'')
            ,[IndexOptions] = isnull( (CAST(@target_IndexOptions collate database_default AS VARCHAR(max))) ,'')
        from [#temp_indexes] cc
        where [object_id] in(@target_objid)
        and [name] in(@target_idx_name)
        ) c
    UNPIVOT (p_value for property in(
             [name]
            ,[type]
            ,[is_unique] 
           -- ,[data_space_id] 
            ,[ignore_dup_key]
            ,[is_primary_key]
            ,[is_unique_constraint]
            ,[fill_factor]
            ,[is_padded]
            ,[is_disabled]
            ,[is_hypothetical] 
            ,[is_ignored_in_optimization]
            ,[allow_row_locks] 
            ,[allow_page_locks] 
            ,[has_filter] 
            ,[compression_delay] 
            ,[suppress_dup_key_messages]
            ,[auto_created]
            ,[key_string]
            ,[include_string]
            ,[filter_dfn]
            ,[IndexOptions]
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

select @PassFail = case when len(@delta_list) > 0 then 'Fail' else 'Pass' end;

begin
  insert into [dbo].[pca_validation_log]([i],[db],[ud_0],[ud_1],[ud_2],[ud_3],[PassFail])
  select 
     [i] = @@servername
    ,[db] = db_name() 
    ,[ud_0] = @now
    ,[ud_1] = N'ix_definition'
    ,[ud_2] = concat('ix: ' ,@target_idx_name ,' ,target: ' ,@target_schema ,'.',@target_name ,' ,source: ' ,@source_schema ,'.',@source_name) 
    ,[ud_3] = @delta_list
    ,[PassFail] = @PassFail;
end
END
end

/*
go
declare @PassFail nchar(4);

exec [dbo].[usp_pca_validate_ix]
   @source_schema = N'pcastage' 
  ,@source_name = N'PC_PATIENTS1'
  ,@target_schema = N'dbo'
  ,@source_idx_name =  N'IX_PC_PATIENTS1_csvid'
  ,@PassFail = @PassFail output

select [@PassFail] = @PassFail;

select * from [dbo].[pca_validation_log];
*/

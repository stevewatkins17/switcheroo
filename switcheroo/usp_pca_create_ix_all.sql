SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO
create or alter proc [dbo].[usp_pca_create_ix_all]
     @source_schema nvarchar(128)
    ,@source_name nvarchar(128)
    ,@mirror_schema nvarchar(128)
    ,@drop_ixes_batch nvarchar(max) output 
    ,@create_ixes_batch nvarchar(max) output
as
begin 
  set @drop_ixes_batch = N'';
  set @create_ixes_batch = N'';

  declare @fq_source_name nvarchar(257) = concat(@source_schema,N'.',@source_name);

  if object_id('[dbo].[pca_ix_properties]') is null
  begin
    create table [dbo].[pca_ix_properties](
	    [fq_source_name] [nvarchar](257) NULL,
	    [object_id] [int] NOT NULL,
	    [name] [nvarchar](128) NULL,
	    [index_id] [int] NOT NULL,
	    [type] [tinyint] NOT NULL,
	    [type_desc] [nvarchar](60) NULL,
	    [is_unique] [bit] NULL,
	    [data_space_id] [int] NULL,
	    [ignore_dup_key] [bit] NULL,
	    [is_primary_key] [bit] NULL,
	    [is_unique_constraint] [bit] NULL,
	    [fill_factor] [tinyint] NOT NULL,
	    [is_padded] [bit] NULL,
	    [is_disabled] [bit] NULL,
	    [is_hypothetical] [bit] NULL,
	    [is_ignored_in_optimization] [bit] NULL,
	    [allow_row_locks] [bit] NULL,
	    [allow_page_locks] [bit] NULL,
	    [has_filter] [bit] NULL,
	    [filter_definition] [nvarchar](max) NULL,
	    [compression_delay] [int] NULL,
	    [suppress_dup_key_messages] [bit] NULL,
	    [auto_created] [bit] NULL   
      ,unique([object_id] ,[index_id])
      ) on [PRIMARY] textimage_on [PRIMARY];
  end
  else
  begin
    delete from [dbo].[pca_ix_properties] where [fq_source_name] = @fq_source_name;
  end

  if object_id('[dbo].[pca_ixc_properties]') is null
  begin
    create table [dbo].[pca_ixc_properties](
	    [fq_source_name] [nvarchar](257) NULL,
	    [ix_name] [nvarchar](128) NULL,
	    [object_id] [int] NOT NULL,
	    [index_id] [int] NOT NULL,
	    [index_column_id] [int] NOT NULL,
	    [key_ordinal] [tinyint] NOT NULL,
	    [partition_ordinal] [tinyint] NOT NULL,
	    [is_descending_key] [bit] NULL,
	    [is_included_column] [bit] NULL,
	    [name] [nvarchar](128) NULL,
	    [column_id] [int] NOT NULL,
	    [system_type_id] [tinyint] NOT NULL,
	    [user_type_id] [int] NOT NULL,
	    [max_length] [smallint] NOT NULL,
	    [precision] [tinyint] NOT NULL,
	    [scale] [tinyint] NOT NULL,
	    [collation_name] [nvarchar](128) NULL,
	    [is_nullable] [bit] NULL,
	    [is_ansi_padded] [bit] NOT NULL,
	    [is_rowguidcol] [bit] NOT NULL,
	    [is_identity] [bit] NOT NULL,
	    [is_computed] [bit] NOT NULL,
	    [is_filestream] [bit] NOT NULL,
	    [is_replicated] [bit] NULL,
	    [is_non_sql_subscribed] [bit] NULL,
	    [is_merge_published] [bit] NULL,
	    [is_dts_replicated] [bit] NULL,
	    [is_xml_document] [bit] NOT NULL,
	    [xml_collection_id] [int] NOT NULL,
	    [default_object_id] [int] NOT NULL,
	    [rule_object_id] [int] NOT NULL,
	    [is_sparse] [bit] NULL,
	    [is_column_set] [bit] NULL,
	    [generated_always_type] [tinyint] NULL,
	    [generated_always_type_desc] [nvarchar](60) NULL,
	    [encryption_type] [int] NULL,
	    [encryption_type_desc] [nvarchar](64) NULL,
	    [encryption_algorithm_name] [nvarchar](128) NULL,
	    [column_encryption_key_id] [int] NULL,
	    [column_encryption_key_database_name] [nvarchar](128) NULL,
	    [is_hidden] [bit] NULL,
	    [is_masked] [bit] NULL,
	    [graph_type] [int] NULL,
	    [graph_type_desc] [nvarchar](60) NULL
      ,unique([object_id] ,[index_id] ,[index_column_id])
    ) on [PRIMARY];
  end
  else
  begin
    delete from [dbo].[pca_ixc_properties] where [fq_source_name] = @fq_source_name;
  end


  insert into [dbo].[pca_ixc_properties]
  select 
      [fq_source_name] = @fq_source_name 
    ,[ix_name] = i.[name]
    ,ic.[object_id]
    ,ic.[index_id]
    ,ic.[index_column_id]
    ,ic.[key_ordinal]
    ,ic.[partition_ordinal]
    ,ic.[is_descending_key]
    ,ic.[is_included_column]
    ,[name] = (CAST(c.[name] collate database_default AS NVARCHAR(max))) 
    ,c.[column_id]
    ,c.[system_type_id]
    ,c.[user_type_id]
    ,c.[max_length]
    ,c.[precision]
    ,c.[scale]
    ,[collation_name] = (CAST(c.[collation_name] collate database_default AS NVARCHAR(max))) 
    ,c.[is_nullable]
    ,c.[is_ansi_padded]
    ,c.[is_rowguidcol]
    ,c.[is_identity]
    ,c.[is_computed]
    ,c.[is_filestream]
    ,c.[is_replicated]
    ,c.[is_non_sql_subscribed]
    ,c.[is_merge_published]
    ,c.[is_dts_replicated]
    ,c.[is_xml_document]
    ,c.[xml_collection_id]
    ,c.[default_object_id]
    ,c.[rule_object_id]
    ,c.[is_sparse]
    ,c.[is_column_set]
    ,c.[generated_always_type] 
    ,[generated_always_type_desc] = (CAST(c.[generated_always_type_desc] collate database_default AS NVARCHAR(max))) 
    ,c.[encryption_type]
    ,[encryption_type_desc] = (CAST(c.[encryption_type_desc] collate database_default AS NVARCHAR(max))) 
    ,[encryption_algorithm_name] = (CAST(c.[encryption_algorithm_name] collate database_default AS NVARCHAR(max))) 
    ,c.[column_encryption_key_id]
    ,[column_encryption_key_database_name] = (CAST(c.[column_encryption_key_database_name] collate database_default AS NVARCHAR(max))) 
    ,c.[is_hidden]
    ,c.[is_masked]
    ,c.[graph_type] 
    ,[graph_type_desc] = (CAST(c.[graph_type_desc] collate database_default AS NVARCHAR(max))) 
  from sys.indexes AS i  
  inner join sys.index_columns AS ic   
      on i.object_id = ic.object_id AND i.index_id = ic.index_id  
      and i.object_id = object_id(@fq_source_name )  
  inner join sys.columns AS c   
      on ic.object_id = c.object_id AND c.column_id = ic.column_id
      and c.object_id = object_id(@fq_source_name ); 

  insert into dbo.pca_ix_properties
  select 
      [fq_source_name] = @fq_source_name 
    ,[object_id]
    ,[name]
    ,[index_id]
    ,[type]
    ,[type_desc]
    ,[is_unique]
    ,[data_space_id]
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
  from sys.indexes i
  where i.[OBJECT_ID] = object_id(@fq_source_name );

/* begin - dynamic idx stmts create/drop - begin */
  /* inputs */
  declare 
       @ix_source_schema nvarchar(128) = @source_schema
      ,@ix_source_name nvarchar(128) = @source_name 
      ,@ix_source_idx_name nvarchar(128);

  declare 
       @ix_target_schema nvarchar(128) = @mirror_schema 
      ,@ix_target_name nvarchar(128) = @source_name  
      ,@ix_target_idx_name nvarchar(128);-- = @ix_source_idx_name; /* non-constraint indexes can have dup names across tables; constraints are unique to DB */

  /*  we begin persisting index create stmts, 
      both physically for backup and locally for drop/create exe */

  declare @ix_source_objid int = object_id(@ix_source_schema + N'.' + @ix_source_name);
  declare @ix_target_objid int = object_id(@ix_target_schema + N'.' + @ix_target_name);

  if @ix_source_objid is null or @ix_target_objid is null
  begin
    throw 50026, 'source or target objid is null; source or target table not exist', 1;
  end

  declare @source_indexes table([id] int identity(0,1) primary key, [ix_name] nvarchar(128) not null, [effective_date] datetimeoffset(3) null ,[ix_drop] nvarchar(max) null ,[ix_create] nvarchar(max) null);

  insert into @source_indexes([ix_name])
  select [name]
  from [sys].[indexes]
  where [object_id] = @ix_source_objid
  and [type] in(1,2)
  order by [type] asc ,[is_primary_key] desc ,[is_unique_constraint] desc ,[is_unique] desc;

  /* global for idx add or drop */
  declare @_idx_id int ,@_idx_threshold int ,@_idx_name nvarchar(128);
  declare @dropIX nvarchar(max) ,@createIX nvarchar(max) ,@disableIX nvarchar(max);

  /* index properties */ 
  declare @isunique nvarchar(6);

  declare 
         @index_id [int]
        ,@idx_type [tinyint]
        ,@type_desc [nvarchar](60)
        ,@is_primary_key [bit]
        ,@is_unique_constraint [bit]
        ,@is_unique [bit]
        ,@is_hypothetical [bit]
        ,@is_disabled [bit] 
        ,@FileGroupName [nvarchar](128);

  declare 
       @string_separator nvarchar(10) = N' ,'
      ,@key_string nvarchar(max)
      ,@include_string nvarchar(max)
      ,@filter_dfn nvarchar(max)
      ,@IndexOptions nvarchar(max);

  declare @index_columns TABLE(
      [index_name] [nvarchar](128) NULL,
      [index_column_id] [int] NOT NULL,
      [key_ordinal] [tinyint] NOT NULL,
      [is_descending_key] [bit] NULL,
      [column_name] [nvarchar](128) NULL,
      [column_type] [nvarchar](128) NULL,
      [is_included_column] [bit] NULL,
      [filter_definition] [nvarchar](4000) NULL,
      [is_identity] [bit] NOT NULL
  );

  set @_idx_id = 0;
  set @_idx_threshold = (select max([id]) +1 from @source_indexes);

  while @_idx_id < @_idx_threshold
  begin
    /* begin def get_create_sql_index */
  set nocount on;

    delete from @index_columns;
    select @dropIX = null ,@createIX  = null ,@disableIX  = null ,@isunique = null;

    select 
       @index_id = null
      ,@idx_type = null
      ,@type_desc = null
      ,@is_primary_key = null
      ,@is_unique_constraint = null
      ,@is_unique = null
      ,@is_hypothetical = null
      ,@is_disabled = null
      ,@FileGroupName = null;

    select 
       @key_string = null
      ,@include_string = null
      ,@filter_dfn = null
      ,@IndexOptions = null;
    
    select @ix_source_idx_name = [ix_name] from @source_indexes i where i.[id] = @_idx_id;
    set @ix_target_idx_name = @ix_source_idx_name;

    
  /* initialize properties */ 
  select 
       @index_id = i.[index_id]
      ,@idx_type = i.[type]
      ,@type_desc = i.[type_desc]
      ,@is_primary_key = i.[is_primary_key]
      ,@is_unique_constraint = i.[is_unique_constraint]
      ,@is_unique = i.[is_unique]
      ,@is_hypothetical = i.is_hypothetical
      ,@is_disabled = i.is_disabled
      ,@FileGroupName = filegroup_name([i].[data_space_id])
  from sys.indexes i
  join sys.objects o ON i.[OBJECT_ID] = o.[OBJECT_ID] 
      and o.[OBJECT_ID] = @ix_source_objid
      and i.[name] = @ix_source_idx_name;

  /*  get strings used to assemble the create index stmt 
      previously - '[create] rowstore index build strings.sql' */
  begin 

      insert into @index_columns
          ([index_name] ,[index_column_id] ,[key_ordinal] ,[is_descending_key] ,[column_name],[column_type],[is_included_column],[filter_definition],[is_identity])
      select 
           [index_name] = [i].[name]
          ,[ic].[index_column_id]  
          ,[ic].[key_ordinal] 
          ,[ic].[is_descending_key] 
          ,[column_name] = [c].name
          ,[column_type] = TYPE_NAME([c].[user_type_id])
          ,[ic].[is_included_column]
          ,[i].[filter_definition] 
          ,[c].[is_identity]
      from sys.indexes AS i  
      inner join sys.index_columns AS ic   
          on i.object_id = ic.object_id AND i.index_id = ic.index_id  
          and i.object_id = @ix_source_objid
          and i.name = @ix_source_idx_name  
      inner join sys.columns AS c   
          on ic.object_id = c.object_id AND c.column_id = ic.column_id; 

      select @IndexOptions = 
	     case when [ix].[is_padded] = 1 then N'pad_index = on, '
			    else N'pad_index = off, '
		    end + case when [ix].[allow_page_locks] = 1 then N'allow_page_locks = on, '
				    else N'allow_page_locks = off, '
				    end + case when [ix].[allow_row_locks] = 1 then N'allow_row_locks = on, '
						    else N'allow_row_locks = off, '
					    end + case when indexproperty(@ix_source_objid, [ix].[name], 'IsStatistics') = 1 then N'statistics_norecompute = on, '
							    else N'statistics_norecompute = off, '
							    end + case when [ix].[ignore_dup_key] = 1 then N'ignore_dup_key = on, '
									    else N'ignore_dup_key = off, '
								    end + N'sort_in_tempdb = off ' + case when [ix].[fill_factor] = 0 then N'' else N',fillfactor =' + cast([ix].[fill_factor] as nvarchar(3)) end

      from sys.indexes AS ix
      where ix.object_id = @ix_source_objid
          and ix.name = @ix_source_idx_name;

      select @key_string = (coalesce(@key_string + @string_separator ,N'') + quotename([column_name]) + space(1) + (case when [is_descending_key] = 0 then N'asc' else N'desc' end) )
      from @index_columns ic
      where ic.key_ordinal > 0
      order by ic.key_ordinal asc;

      select @include_string = (coalesce(@include_string + @string_separator ,N'') + quotename([column_name]) )
      from @index_columns ic
      where ic.key_ordinal = 0
      order by ic.index_column_id asc;

      select top 1 @filter_dfn = [filter_definition] from @index_columns;
    
  end

  /* UC #1: create PK constraint */
  if @is_primary_key = 1
  begin
    set @dropIX = N'
  alter table [' + @ix_target_schema + N'].[' + @ix_target_name + N'] drop constraint if exists [' + @ix_target_idx_name + N'];
  '
    set @createIX = N'
  alter table [' + @ix_target_schema + N'].[' + @ix_target_name + N'] 
    add constraint [' + @ix_target_idx_name + N'] primary key '+ @type_desc + N'(' + @key_string + N') 
    with (' + @IndexOptions + N');
      '; ---- on [' + lower(@FileGroupName) + N'];

  end 
 
  /* UC #2: create UC constraint */
  if @is_unique_constraint = 1
  begin
    set @dropIX = N'
  alter table [' + @ix_target_schema + N'].[' + @ix_target_name + N'] drop constraint if exists [' + @ix_target_idx_name + N'];
    ';
    set @createIX = N'
  alter table [' + @ix_target_schema + N'].[' + @ix_target_name + N'] 
    add constraint [' + @ix_target_idx_name + N'] unique '+ @type_desc + N'(' + @key_string + N') 
    with (' + @IndexOptions + N');
    ';-- on [' + lower(@FileGroupName) + N']

  end 
 
  /* UC #3: create plain old non-constraint idx */
  else 
      begin
        if @is_primary_key = 0 and @is_unique_constraint = 0
        begin
            set @isunique = (case when @is_unique = 1 then N'unique' else N'' end);

            set @dropIX = N'
  drop index if exists ' + QUOTENAME(@ix_target_idx_name) + N' on ' + QUOTENAME(@ix_target_schema) + N'.' + QUOTENAME(@ix_target_name) + N';
              ';


            set @createIX = N'
  create ' + isnull(@isunique ,N'') + space(1) + lower(@type_desc) + N' index ' + QUOTENAME(@ix_target_idx_name) + N' on ' + QUOTENAME(@ix_target_schema) + N'.' + QUOTENAME(@ix_target_name) + N'
    (' + @key_string + N') ' 
  + case when @include_string is not null then N'
    include(' + @include_string + N') ' else N'' end 
  + case when @filter_dfn is not null then N'
    where ' + @filter_dfn else N'' end 
  + N'
    with (' + @IndexOptions + N') 
    ;
  ';--on [' + lower(@FileGroupName) + N']

        end
    end

  if @is_disabled = 1
  begin
    set @disableIX = N'
  alter index [' + @ix_target_idx_name + N'] on [' + @ix_target_schema + N'].[' + @ix_target_name + N'] disable;
  ';

    set @createIX += @disableIX; 
  end
  /* end def get_create_sql_index */

  begin try
    if @dropIX is null or @createIX is null /* critical error */
    begin 
      select [target_idx_name] = @ix_target_idx_name ,[dropIX] = @dropIX ,[createIX] = @createIX;
      throw 50027,'error: null index drop or create stmt', 1;
    end 
    else
    begin
      update @source_indexes
      set [effective_date] = SYSDATETIMEOFFSET() ,[ix_drop] = @dropIX ,[ix_create] = @createIX
      where [id] = @_idx_id;

      set @drop_ixes_batch += @dropIX;
      set @create_ixes_batch += @createIX;
    end
  end try
  begin catch
    SELECT
      ERROR_NUMBER() AS ErrorNumber,
      ERROR_SEVERITY() AS ErrorSeverity,
      ERROR_STATE() AS ErrorState,
      ERROR_PROCEDURE() AS ErrorProcedure,
      ERROR_LINE() AS ErrorLine,
      ERROR_MESSAGE() AS ErrorMessage;

      throw;
  end catch

  set @_idx_id += 1;
end
  /* end - dynamic idx stmts create/drop - end */

end
/* end - persist ix snapshots for bu - end */

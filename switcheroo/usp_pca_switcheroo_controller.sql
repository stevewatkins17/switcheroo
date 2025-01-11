SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO
create or alter procedure [dbo].[usp_pca_switcheroo_controller]
   @source_schema nvarchar(128)
  ,@source_name nvarchar(128)
  ,@stage_schema nvarchar(128)
  ,@mirror_schema nvarchar(128)
  ,@withSwitch bit = 0
as
begin
  set nocount on;

  if object_id(concat(@source_schema,N'.',@source_name)) is null
  begin
    declare @msg17 nvarchar(4000) = concat(N'ERROR: source dbo.table not exist: ' ,@source_schema ,N'.',@source_name);
    throw 50017, @msg17, 1;
  end

  if object_id(concat(@stage_schema,N'.',@source_name)) is not null
  begin
    declare @msg27 nvarchar(4000) = concat(N'ERROR: stage.table pre-exists: ' ,@source_schema ,N'.',@source_name);
    throw 50027, @msg27, 1;
  end

  declare @run_stmt nvarchar(max);

  begin
    /* create_schema */
    select @run_stmt = [stmt] from [dbo].[tvp_pca_create_schema](@stage_schema); 

    insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg])
      values('tvp_pca_create_schema-stage' ,@stage_schema ,@source_name ,SYSDATETIMEOFFSET() ,@run_stmt); 

    exec sp_executesql @run_stmt;

    set @run_stmt = null;
    select @run_stmt = [stmt] from [dbo].[tvp_pca_create_schema](@mirror_schema); 

    insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg])
      values('tvp_pca_create_schema-mirror' ,@mirror_schema ,@source_name ,SYSDATETIMEOFFSET() ,@run_stmt); 
    
    exec sp_executesql @run_stmt;

    /* create_table */
    set @run_stmt = null;
    select @run_stmt = [stmt] from  [dbo].[tvp_pca_create_table](@source_schema,@source_name ,@mirror_schema) x where x.stmt_type = N'DropMirrorTable';

    insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg])
      values('tvp_pca_create_table-drop' ,@mirror_schema ,@source_name ,SYSDATETIMEOFFSET() ,@run_stmt); 

    exec sp_executesql @run_stmt;

    set @run_stmt = null;
    select @run_stmt = [stmt] from  [dbo].[tvp_pca_create_table](@source_schema,@source_name ,@mirror_schema) x where x.stmt_type = N'CreateMirrorTable';

    insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg])
      values('tvp_pca_create_table-create' ,@mirror_schema ,@source_name ,SYSDATETIMEOFFSET() ,@run_stmt); 

    exec sp_executesql @run_stmt;

    /* we have a known issue with "create_XP" */
    /* create_XP */

    set @run_stmt = null;
    exec [dbo].[usp_pca_create_xp_all]
       @source_schema = @source_schema 
      ,@source_name = @source_name
      ,@target_schema = @mirror_schema
      ,@stmt_batch = @run_stmt output;


    insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg])
      values('usp_pca_create_xp_all' ,@mirror_schema ,@source_name ,SYSDATETIMEOFFSET() ,@run_stmt); 

    exec sp_executesql @run_stmt;
    
    /* create_ix */
    set @run_stmt = null;
    declare @drop_ixes_batch nvarchar(max) ,@create_ixes_batch nvarchar(max);

    exec [dbo].[usp_pca_create_ix_all]
         @source_schema = @source_schema
        ,@source_name = @source_name
        ,@mirror_schema = @mirror_schema
        ,@drop_ixes_batch = @drop_ixes_batch output 
        ,@create_ixes_batch = @create_ixes_batch output

    set @run_stmt = @drop_ixes_batch;

    insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg])
      values('sp_pca_create_ix_all-drop' ,@mirror_schema ,@source_name ,SYSDATETIMEOFFSET() ,@run_stmt); 

    exec sp_executesql @run_stmt;

    set @run_stmt = null;
    set @run_stmt = @create_ixes_batch;

    insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg])
      values('sp_pca_create_ix_all-create' ,@mirror_schema ,@source_name ,SYSDATETIMEOFFSET() ,@run_stmt); 

    exec sp_executesql @run_stmt;

    /*create DFs */

    set @run_stmt = null;
    exec [dbo].[usp_pca_create_df_all]
       @source_schema = @source_schema 
      ,@source_name = @source_name
      ,@target_schema = @mirror_schema 
      ,@create_DF_batch = @run_stmt output;


    insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg])
      values('usp_pca_create_df_all' ,@mirror_schema ,@source_name ,SYSDATETIMEOFFSET() ,@run_stmt); 

    exec sp_executesql @run_stmt;

    /* reseed table identity column */

    set @run_stmt = null;
    exec [dbo].[usp_pca_reset_seed]
     @source_schema = @source_schema 
    ,@source_name  = @source_name
    ,@mirror_schema = @mirror_schema 
    ,@stmt = @run_stmt output;

    insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg])
      values('usp_pca_reset_seed' ,@mirror_schema ,@source_name ,SYSDATETIMEOFFSET() ,@run_stmt); 

    if @run_stmt is not null
    begin
      exec sp_executesql @run_stmt;
    end

    if @withSwitch = 1
    begin
      /* schema xfer dbo-to-stage */ 
      set @run_stmt = ([dbo].[fn_pca_schema_xfer](@source_schema, @stage_schema ,@source_name));

      insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg])
        values('fn_pca_schema_xfer-dbo-to-stage' ,@mirror_schema ,@source_name ,SYSDATETIMEOFFSET() ,@run_stmt); 

      exec sp_executesql @run_stmt;

      /* schema xfer mirror-to-dbo */
      set @run_stmt = ([dbo].[fn_pca_schema_xfer](@mirror_schema ,@source_schema,@source_name));

      insert into [dbo].[pca_log]([event_name] ,[obj_schema_name] ,[obj_name] ,[insert_ts] ,[msg])
        values('fn_pca_schema_xfer-mirror-to-dbo' ,@mirror_schema ,@source_name ,SYSDATETIMEOFFSET() ,@run_stmt); 

      exec sp_executesql @run_stmt;
    end

  end
end

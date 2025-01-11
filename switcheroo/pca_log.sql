drop table if exists [dbo].[pca_log];
GO
SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO
begin
  create table [dbo].[pca_log](
	    [rid] int identity(0,1) not null 
    ,[event_name] [nvarchar](256) not null
	  ,[obj_schema_name] [nvarchar](128) null
	  ,[obj_name] [nvarchar](128) null
    ,[insert_ts] datetimeoffset not null
    ,[msg] [nvarchar](max) null
    );
end
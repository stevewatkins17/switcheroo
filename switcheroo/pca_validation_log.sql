drop table if exists [dbo].[pca_validation_log];
GO
SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO

create table [dbo].[pca_validation_log](
	 [i] nvarchar(128) not null
	,[db] nvarchar(128) not null
	,[ud_0] datetimeoffset null
	,[ud_1] nvarchar(4000) null
	,[ud_2] nvarchar(4000) null
	,[ud_3] nvarchar(4000) null
	,[PassFail] nchar(4) null
);
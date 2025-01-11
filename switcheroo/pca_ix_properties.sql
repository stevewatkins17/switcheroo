drop table if exists [dbo].[pca_ix_properties];
GO
SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[pca_ix_properties](
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
	[auto_created] [bit] NULL,
UNIQUE NONCLUSTERED 
(
	[object_id] ASC,
	[index_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
);




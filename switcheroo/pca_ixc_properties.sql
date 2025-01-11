drop table if exists [dbo].[pca_ixc_properties];
GO
SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[pca_ixc_properties](
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
	[graph_type_desc] [nvarchar](60) NULL,
UNIQUE NONCLUSTERED 
(
	[object_id] ASC,
	[index_id] ASC,
	[index_column_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
)


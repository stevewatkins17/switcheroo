select * from [dbo].[pca_ix_properties]
order by [object_id]	,[index_id]	

 select * from [dbo].[pca_ixc_properties]
 order by [object_id]	,[index_id]	,[index_column_id]

 --drop table if exists [dbo].[pca_ixc_properties];
 
 --drop table if exists [dbo].[pca_ix_properties];
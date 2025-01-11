SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO
create or alter function [dbo].[fn_pca_schema_xfer] (@from_schema nvarchar(128), @to_schema nvarchar(128), @tablename nvarchar(128))
  returns nvarchar(4000)
as 
begin
	declare @SCHEMA_xfer_stmt nvarchar(4000) = N'ALTER SCHEMA [' + @to_schema + N'] TRANSFER [' + @from_schema + N'].[' + @tablename + N'];';
	return @SCHEMA_xfer_stmt;
end
SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE or ALTER FUNCTION [dbo].[tvp_pca_create_schema](
     @schemaname nvarchar(128)
) 
RETURNS @T TABLE([stmt_type] NVARCHAR(128) ,[stmt] NVARCHAR(4000)) 
AS 
BEGIN 

  insert into @T([stmt_type] ,[stmt]) values
  (N'createschema' ,N'
if not exists(select 1 from sys.schemas where name = ''' + @schemaname + N''') 
begin 
  declare @cr_change_schema nvarchar(128) = ''create schema ' + @schemaname + N';''; 
  exec sys.sp_executesql @cr_change_schema; 
end
');

return
END

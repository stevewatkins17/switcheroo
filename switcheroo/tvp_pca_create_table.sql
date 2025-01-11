SET ANSI_WARNINGS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE or ALTER FUNCTION [dbo].[tvp_pca_create_table](
     @source_schema nvarchar(128)
    ,@source_name nvarchar(128)
    ,@mirror_schema nvarchar(128)
) 
RETURNS @T TABLE([stmt_type] NVARCHAR(128) ,[stmt] NVARCHAR(max)) 
AS 
BEGIN 
  declare @source_objid int = (select [object_id] from sys.tables o join sys.schemas s on o.[schema_id] = s.[schema_id] and s.[name] = @source_schema and o.name = @source_name);

  /* drop-create-alter mirror table: {mirror_schema}.{source_name} */  
  declare 
     @drop_table_sql nvarchar(500) = N'DROP TABLE if exists ['+ @mirror_schema + N'].[' + @source_name + N'];'
    ,@object_name nvarchar(257) = @source_schema + N'.' + @source_name;

  declare @create_table_sql NVARCHAR(MAX) = N'CREATE TABLE [' + @mirror_schema + N'].[' + @source_name + N'] ' + NCHAR(13) + N'(' + NCHAR(13) + STUFF((  
    SELECT NCHAR(13) + N'     ,[' + c.name + N'] ' +   
        CASE WHEN c.is_computed = 1  
            THEN N'AS ' + OBJECT_DEFINITION(c.[object_id], c.column_id)  
                  + CASE WHEN cc.[is_persisted] = 1 THEN (SPACE(1) + 'persisted') ELSE '' END
                  + CASE WHEN cc.[is_persisted] = 1 and cc.is_nullable = 0 THEN N' NOT NULL' ELSE N'' END                      
            ELSE   
                CASE WHEN c.system_type_id != c.user_type_id   
                    THEN N'[' + SCHEMA_NAME(tp.[schema_id]) + N'].[' + tp.name + N']'   
                    ELSE N'[' + UPPER(tp.name) + N']'   
                END  +   
                CASE   
                    WHEN tp.name IN ('varchar', 'char', 'varbinary', 'binary')  
                        THEN N'(' + CASE WHEN c.max_length = -1   
                                        THEN 'MAX'   
                                        ELSE CAST(c.max_length AS NVARCHAR(24))   
                                    END + N')'  
                    WHEN tp.name IN ('nvarchar', 'nchar')  
                        THEN N'(' + CASE WHEN c.max_length = -1   
                                        THEN N'MAX'   
                                        ELSE CAST(c.max_length / 2 AS NVARCHAR(24))   
                                    END + N')'  
                    WHEN tp.name IN ('datetime2', 'time', 'datetimeoffset')   
                        THEN N'(' + CAST(c.[scale] AS NVARCHAR(24)) + N')'  
                    WHEN tp.name in('decimal' ,'numeric')  
                        THEN N'(' + CAST(c.[precision] AS NVARCHAR(24)) + N',' + CAST(c.scale AS NVARCHAR(24)) + N')'  
                    ELSE ''  
                END +  
                CASE WHEN c.collation_name IS NOT NULL AND c.system_type_id = c.user_type_id   
                    THEN N' COLLATE ' + c.collation_name  
                    ELSE N''  
                END  +    
                CASE WHEN x.[name] is not null 
                    THEN concat(N'(CONTENT [' ,schema_name(x.[schema_id]) ,N'].[' ,x.[name] ,N']) ' )
                    ELSE N''   
                END +  
                CASE WHEN c.is_nullable = 1   
                    THEN N' NULL'  
                    ELSE N' NOT NULL'  
                END +    
                CASE WHEN c.is_identity = 1 
                    THEN N' IDENTITY(' + CAST(IDENTITYPROPERTY(c.[object_id], 'SeedValue') AS NVARCHAR(24)) + N',' +   
                                    CAST(IDENTITYPROPERTY(c.[object_id], 'IncrementValue') AS NVARCHAR(24)) + N')'   
                    ELSE N''   
                END +
                CASE WHEN ic.[is_not_for_replication] = 1 
                    THEN N' NOT FOR REPLICATION '   
                    ELSE N''   
                END +    
                CASE WHEN c.[is_rowguidcol] = 1 
                    THEN N' ROWGUIDCOL '   
                    ELSE N''   
                END                
        END  
    FROM sys.columns c   --NOT FOR REPLICATION and c.[is_not_for_replication] = 0
    JOIN sys.types tp ON c.user_type_id = tp.user_type_id 
    LEFT JOIN sys.computed_columns cc on c.[object_id] = cc.[object_id] and c.[column_id] = cc.[column_id] and c.[is_computed] = 1 and cc.[is_persisted] = 1  
    LEFT JOIN [sys].[identity_columns] ic on c.[object_id] = ic.[object_id] and c.[column_id] = ic.[column_id] and ic.[is_not_for_replication] = 1  
    left join [sys].[xml_schema_collections] x on x.[xml_collection_id] = c.[xml_collection_id]
    WHERE c.[object_id] = @source_objid  
    ORDER BY c.column_id  
    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 7, '      ') + NCHAR(13) + N') '  
  
  /*declare @FG nvarchar(128) = N' ON [Third];'

  set @create_table_sql += @FG;
  */

  set @create_table_sql += N';';

  if exists(select 1 from sys.tables t
              where t.[object_id] = @source_objid
              and t.[lock_escalation] = 1)
  begin
    set @create_table_sql += N'

ALTER TABLE [' + @mirror_schema + N'].[' + @source_name + N'] SET (LOCK_ESCALATION = DISABLE); 
';
  end

  insert into @T([stmt_type] ,[stmt]) values
   (N'DropMirrorTable' ,@drop_table_sql)
  ,(N'CreateMirrorTable' ,@create_table_sql)

  RETURN
END
/*
go
-- [dbo].[CBSA2021_Hospice]
-- [tstmirrordbo].[CBSA2021_Hospice]
-- [tstmirrordbo].[MEL_DRUGS]
    -- N'Billing'
    --,N'CLAIM_PROFILE_FORMATS'
    -- dbo.AUDIT_SCHED
    -- [Sales].[Store]
-- source_column_name: li_durationinhours target: tstmirrorBilling.LINE_ITEMS, source: Billing.LINE_ITEMS

declare @CreateMirrorTable nvarchar(max) = (

select [stmt] from
[dbo].[tvp_pca_create_table](
     N'Sales'
    ,N'Store'
    ,N'tstmirrorSales'
) x
where x.[stmt_type] = N'CreateMirrorTable')
;

--print(@CreateMirrorTable);
select (convert(xml ,@CreateMirrorTable));

*/
/*
select object_schema_name(c.[object_id]) ,cc.[is_persisted] ,cc.* 
    FROM sys.columns c   --NOT FOR REPLICATION and c.[is_not_for_replication] = 0
    JOIN sys.types tp ON c.user_type_id = tp.user_type_id 
    LEFT JOIN sys.computed_columns cc on c.[object_id] = cc.[object_id] and c.[column_id] = cc.[column_id] and c.[is_computed] = 1 and cc.[is_persisted] = 1  
    LEFT JOIN [sys].[identity_columns] ic on c.[object_id] = ic.[object_id] and c.[column_id] = ic.[column_id] and ic.[is_not_for_replication] = 1  
    WHERE c.[object_id] in( object_id('Billing.LINE_ITEMS') ,object_id('tstmirrorBilling.LINE_ITEMS'))
    and c.[name] = 'li_durationinhours'
    ORDER BY c.column_id 
*/

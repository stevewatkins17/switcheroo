--:connect PBSQL891
--use HCHB_AMEDISYS;
:connect PBSQL409
use HCHB_NEWENVIRON;
	

--:connect XBISQL842.hchb.local,1433
--use [TEMP_ABSOLUTE_20241031113137710_STEVEWATKINS];

/* get PC "sessnum" -- table scope  */
declare @scoped_tables table ([SchemaName] nvarchar(128) ,[TableName] nvarchar(128) ,[ObjectID] int ,primary key([SchemaName] ,[TableName]));

insert into @scoped_tables ([SchemaName] ,[TableName] ,[ObjectID])
SELECT 
   [SchemaName] = SCHEMA_NAME(s1.[schema_id])
  ,[TableName] = s1.[name]
  ,[ObjectID] = s1.[object_id]
    FROM sys.objects s1
    WHERE s1.[type] = 'U' 
        AND s1.[name] = 'SCHED'
        --AND LEFT(s1.[name], 3) = 'PC_'
        --AND EXISTS (SELECT 1 FROM sys.columns s2 WHERE s2.OBJECT_ID = s1.OBJECT_ID AND s2.[name] = 'agid')
        --AND EXISTS (SELECT 1 FROM sys.columns s2 WHERE s2.OBJECT_ID = s1.OBJECT_ID AND s2.[name] = 'sessnum')
        --AND s1.[name] NOT IN ('PC_CACHE_AGENTSPATIENTS_AP', 'PC_CACHE_PC_EPISODES_AP');

select [PC tables in scope] = count(*) from @scoped_tables;
select * from @scoped_tables;

/* get PC "sessnum" -- child object types */
with all_types as(
  select 
    distinct t.[type_desc]
  from sys.objects t
  where t.[is_ms_shipped] = 0
)
, cte as(
select 
-- [parent_schema] = object_schema_name(t.[parent_object_id])
--,[parent_table] = object_name(t.[parent_object_id])
--,* 
   t.[type_desc]
  ,[PC_type_count] = count(*)
from sys.objects t
where exists(select 1 from @scoped_tables st where (st.[ObjectID] = t.[parent_object_id]))
group by t.[type_desc]
)
select 
 [child_type_desc] = c.[type_desc]
,c.[PC_type_count]
from cte c
union 
select 
   aty.[type_desc] 
  ,[PC_type_count] = 0
from [all_types] aty 
where not exists(select 1 from cte c where aty.[type_desc] = c.[type_desc])
order by [PC_type_count] desc;

/* get PC "sessnum" -- index types */
with cte as(
select 
 [parent_schema] = object_schema_name(i.[object_id])
,[parent_table] = object_name(i.[object_id])
,* 
from sys.indexes i
where exists(select 1 from @scoped_tables st where (st.[ObjectID] = i.[object_id]))
--and i.[index_id] > 0
)
select 
 [idx_type_desc] = c.[type_desc]
,[is_primary_key]
,[is_unique_constraint]
,count(*) 
from cte c
group by 
 [type_desc]
,[is_primary_key]
,[is_unique_constraint]
;
/* FK detail */

select 
 [parent_table_schemaname] = object_schema_name(fk.[parent_object_id])
,[parent_table_name] = object_name(fk.[parent_object_id])
,[referenced_table_schemaname] = object_schema_name(fk.[referenced_object_id])
,[referenced_table_name] = object_name(fk.[referenced_object_id])
,[name]
from sys.foreign_keys fk
where exists(select 1 from @scoped_tables t where t.[ObjectID] = fk.[parent_object_id])
order by 1,2,3,4;

select 
 [parent_table_schemaname] = object_schema_name(fk.[parent_object_id])
,[parent_table_name] = object_name(fk.[parent_object_id])
,[referenced_table_schemaname] = object_schema_name(fk.[referenced_object_id])
,[referenced_table_name] = object_name(fk.[referenced_object_id])
,[name]
from sys.foreign_keys fk
where exists(select 1 from @scoped_tables t where t.[ObjectID] = fk.[referenced_object_id])
order by 1,2,3,4;

/* get PC "sessnum" -- change tracking */
with cte as(
select 
 [parent_schema] = object_schema_name(ctt.[object_id])
,[parent_table] = object_name(ctt.[object_id])
,* 
from sys.change_tracking_tables ctt
where exists(select 1 from @scoped_tables st where (st.[ObjectID] = ctt.[object_id]))
)
select 
*
from cte c;

/* get PC "sessnum" -- seed identities */
with cte as(
select 
 [parent_schema] = object_schema_name(ic.[object_id])
,[parent_table] = object_name(ic.[object_id])
,* 
from sys.identity_columns ic
where exists(select 1 from @scoped_tables st where (st.[ObjectID] = ic.[object_id]))
) , nv as(
select 
--[next_value] = isnull( (convert(int ,c.[last_value]) + 1) ,convert(int ,c.[seed_value]))
[next_value] = (convert(bigint ,c.[last_value]) + 1) 
,*
from cte c
where c.[last_value] is not null
)
select [reseed_ident] = concat('DBCC CHECKIDENT(''[' ,[parent_schema] , N'].[' , [parent_table] , N']'', RESEED ,' , [next_value] , ');')
from nv
;

/* get PC "sessnum" -- XP */
with cte as(
select 
 [parent_schema] = object_schema_name(extp.[major_id])
,[parent_table] = object_name(extp.[major_id])
,* 
    from sys.[extended_properties] extp
    where extp.[class] in(1,7) 
    and exists(select 1 from @scoped_tables st where (extp.[major_id] = st.ObjectID))
)
select 
*
from cte c
;

/* schema-binding*/
SELECT 
     [SchemaName] = schema_name(t.[schema_id])
    ,[BoundTableName] = t.[name] 
FROM sys.tables t
INNER JOIN sys.sql_dependencies d ON t.[object_id] = d.referenced_major_id
and t.is_ms_shipped = 0 
AND d.class = 1
and exists(select 1 from @scoped_tables st where t.[object_id] = st.ObjectID)
group by t.[schema_id],t.[name];

/* xact repl */
  if exists(select 1 from [master].[sys].[databases] where [is_published] = 1 AND [name] = db_name())
     and 2 = (select count(*) from sys.objects where [name] in('syspublications' ,'sysarticles'))
  begin
    select 
     [PubName] = p.name
    --,[ArticleName] = a.name 
    ,[count]= count(*)
    from dbo.syspublications p JOIN dbo.sysarticles a ON a.pubid = p.pubid 
    and p.status = 1
    and exists(select 1 from @scoped_tables st where a.[objid] = st.ObjectID)
    group by p.name
	end
  
go

/* 
 use TEMP_AMEDISYS_WeeklyRefresh;
select count(*) from dbo.PC_CLINICIANETATRACKING
select count(*) from dbo.PC_ELECTIONADDENDUMREQUESTCONTACT
select count(*) from dbo.PC_ELECTIONADDENDUMREQUESTEDSTATUS
select count(*) from dbo.PC_ELECTIONADDENDUMREQUESTS
select count(*) from dbo.PC_PATIENTCONTACTINFO
select count(*) from dbo.PC_PATIENTMEDICATIONSETUP
select count(*) from dbo.PC_POINTCARECONNECTIVITYLOGS
select count(*) from dbo.PC_POINTCAREIPV4LOGS


select top 1000 * from dbo.PC_POINTCARECONNECTIVITYLOGS
select top 1000 * from dbo.PC_POINTCAREIPV4LOGS

select min(insertdate) from dbo.PC_POINTCARECONNECTIVITYLOGS
select min(insertdate) from dbo.PC_POINTCAREIPV4LOGS


select agid ,sessnum ,[mycount] = count(*)
into #t
from dbo.PC_PATIENTS1
group by agid ,sessnum
having count(*) > 1;

select sessnum ,[mycount] = count(*)
into #t1
from dbo.PC_PATIENTS1
group by sessnum
having count(*) > 1;

select sessnum ,[mycount] = count(*)
into #t2
from #t
group by sessnum
having count(*) > 1;

with cte as(
select top 1 *
from #t 
where exists(select 1 from #t2 where #t.sessnum = #t2.sessnum)
order by newid()
)
select top 1000 * from dbo.PC_PATIENTS1 t
where exists(select 1 from cte c where c.sessnum = t.sessnum)
order by sessnum,agid ,patientid ,csvid;



select top 1000 * from dbo.PC_PATIENTS1;
*/

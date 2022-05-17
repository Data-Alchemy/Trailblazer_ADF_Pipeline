Declare @psize				   int;
Declare @filter				   varchar(max);
Declare @filter_action   varchar(max);




set @psize				    = 1000000; -- set to partition row size you want for each chunk
set @filter				    = 'Value'; -- if present in table then add filter condition, add your condition here 
set @filter_action		= 'where my filter = True'; -- my filter condition , add your where clause here


USE {MY_DATABASE}; -- add your database name here

With filters as 
(
Select 
case 
	when  UPPER(TRIM(COLUMN_NAME))	like '%'+@filter+'%'then	''''+@filter_action+'''' 
end																								                        as filter_statement,
* 
from  INFORMATION_SCHEMA.COLUMNS
where TABLE_NAME LIKE '%StaplesUS_HIT%' or TABLE_NAME LIKE '%StaplesUS_Page%'
),


filter_conditions as (
SELECT  TABLE_CATALOG CAT,
		TABLE_SCHEMA  SCH,
		TABLE_NAME    TBL,
		max(COLUMN_NAME) as COL,
 STRING_AGG(filter_statement, ' and ') WITHIN GROUP (ORDER BY TABLE_NAME) AS Filter_Statements
  FROM filters

 GROUP BY	TABLE_CATALOG,
			TABLE_SCHEMA,
			TABLE_NAME
),

subject_areas as 
(
Select
TABLE_CATALOG									CAT,
TABLE_SCHEMA									SCH,
TABLE_NAME										TBL,
max(isnull(case 
              when upper(TABLE_NAME) like '%RETAIL%'	then 'retail-data'
           end,'retail-data'))	                                                      as subject_area
from   [INFORMATION_SCHEMA].[COLUMNS]
group by TABLE_CATALOG,									
TABLE_SCHEMA	,								
TABLE_NAME
),


base as (
Select TABLE_CATALOG																				                            as DATABASE_NAME,
TABLE_SCHEMA																						                                as TABLE_SCHEMA,
TABLE_NAME																							                                as TABLE_NAME,
COLUMN_NAME																							                                as 'source.name',
DW_Orders.[dbo].[ReplaceASCII](replace([COLUMN_NAME],'value','raw_value'))							as 'sink.name',
case 
	when PATINDEX('%[^_0-9A-Za-z]%',COLUMN_NAME)>1 then 'True' 
end																									                                    as Uncompatible_Column,
sub.subject_area																					                              as TARGET_SUBJECT_AREA,
 fltr.Filter_Statements																				                          as filter_statement,
'{"DATABASE_NAME":"'+TABLE_CATALOG+'",'+'"TABLE_SCHMEA":"'+TABLE_SCHEMA+'",'+'"TABLE_NAME":"'+TABLE_NAME+'",'+'"TARGET_SUBJECT_AREA":"'+sub.subject_area+'",'+'"FILTER_STATEMENT":"'+
''
+'"}'																								                                    as JSON_OBJECT,
CURRENT_TIMESTAMP																					                              as CREATED_ON,
CAST( GETDATE() AS Date )																			                          as EFFECTIVE_DATE,
CURRENT_TIMESTAMP                                                                       as LAST_MODIFIED,
case when upper(TABLE_NAME) like '%NOT IN USE%' then 0
when upper(TABLE_NAME) like '%ADF_CONTROL%' then 0
else 1
end																									                                    as ACTIVE,
''																									                                    as COMMENT
from [INFORMATION_SCHEMA].[COLUMNS]													                            as Sch
left outer join filter_conditions																	                      as fltr 
	on Sch.TABLE_CATALOG	= fltr.CAT
	and Sch.TABLE_SCHEMA	= fltr.SCH
	and Sch.TABLE_NAME		= fltr.TBL
	and Sch.COLUMN_NAME		= fltr.COL
left join subject_areas																		                              as sub 
	on Sch.TABLE_CATALOG	= sub.CAT
	and Sch.TABLE_SCHEMA	= sub.SCH
	and Sch.TABLE_NAME		= sub.TBL
where TABLE_NAME LIKE '%StaplesUS_HIT%' or TABLE_NAME LIKE '%StaplesUS_Page%'						
),


main as (

Select DATABASE_NAME                                    as DATABASE_NAME, 
table_schema                                            as table_schema, 
table_name                                              as table_name,
Target_Subject_Area                                     as Target_Subject_Area,

'{"type": "TabularTranslator", "mappings":['+STRING_AGG(cast('{"source":{"name":"'+[source.name]+'"},"sink":{"name":"'+[sink.name]+'"}}' as NVARCHAR(MAX)),',')+']}'
 column_mapping
from base 
group by DATABASE_NAME, TAble_schema, table_name,Target_Subject_Area
),

Row_counts AS (
SELECT TOP 1000
        a3.name																						AS SchemaName,
        a2.name																						AS TableName,
        a1.rows																						AS Row_Count,
        (a1.reserved )* 8.0 / 1024																	AS reserved_mb,
        a1.data * 8.0 / 1024																		AS data_mb,
        (CASE WHEN (a1.used ) > a1.data THEN (a1.used ) - a1.data ELSE 0 END) * 8.0 / 1024			AS index_size_mb,
        (CASE WHEN (a1.reserved ) > a1.used THEN (a1.reserved ) - a1.used ELSE 0 END) * 8.0 / 1024	AS unused_mb
    FROM    (   SELECT
                ps.object_id,
                SUM ( CASE WHEN (ps.index_id < 2) THEN row_count    ELSE 0 END )					AS rows,
                SUM (ps.reserved_page_count) AS reserved,
                SUM (CASE   WHEN (ps.index_id < 2) THEN (ps.in_row_data_page_count + ps.lob_used_page_count + ps.row_overflow_used_page_count)
                            ELSE (ps.lob_used_page_count + ps.row_overflow_used_page_count) END
                    ) AS data,
                SUM (ps.used_page_count) AS used
                FROM sys.dm_db_partition_stats ps
                GROUP BY ps.object_id
            ) AS a1
    INNER JOIN sys.all_objects a2  ON ( a1.object_id = a2.object_id )
    INNER JOIN sys.schemas a3 ON (a2.schema_id = a3.schema_id)
    WHERE a2.type <> N'S' and a2.type <> N'IT'   
    order by a1.data desc   )

Select main.*,
fc.Filter_Statements							as dynamic_filter,
format(rc.Row_Count,'N0','en-us')				as row_count,
cast (round(rc.Row_Count/@psize,2,1) as int)	as partititons
from main
left join Row_counts rc on rc.SchemaName = main.table_schema
		and rc.TableName =  main.table_name
left join filter_conditions fc on fc.CAT = main.DATABASE_NAME
		and fc.SCH = main.TABLE_SCHEMA
		and fc.TBL = main.TABLE_NAME
;

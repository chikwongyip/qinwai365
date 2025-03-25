select
    *
from
    SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
where
    database_name = 'ODS'
    and query_text LIKE '%ODS.CRM.ODS_T_STORE%'
    and start_time >= '2025-03-06 12:00:00'
order by
    start_time DESC;

select
    *
from
    SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
where
    database_name = 'ODS'
    and query_text LIKE '%ODS.CRM.ODS_T_STORE%'
    and start_time >= '2025-03-06 12:00:00'
    and query_type = 'TRUNCATE_TABLE'
    and user_name = 'SYSTEM'
order by
    start_time DESC;

select
    *
from
    SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
    join SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah on qh.query_id = ah.query_id
where
    qh.query_id = '01bad42e-3201-7b1c-0002-43c610e887e6';

-- 替换为实际 Query IDSELECT query_text, completed_time
select
    *
from
    snowflake.account_usage.task_history
order by
    completed_time DESC;
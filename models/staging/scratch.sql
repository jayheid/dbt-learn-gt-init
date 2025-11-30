select 
    table_type
    table_schema,
    table_name,
    last_altered,
    case when table_type = 'VIEW' then 'VIEW' else 'TABLE' end as object_type,
    'DROP ' || object_type || ' ' || '{{ database | upper }}' || '.' || table_schema || '.' || table_name || ';' as drop_statement
from {{ database }}.information_schema.tables
where 1=1 
    and table_schema = upper('{{ schema }}')
    and date(last_altered) <= date(dateadd('day', -2, current_date))
order by 1 desc

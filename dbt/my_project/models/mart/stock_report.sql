{{ config(
    materialized="table"
) }}

select
    symbol,
    count(*)            as total_records,
    min(timestamp)      as first_timestamp,
    max(timestamp)      as last_timestamp,
    avg(close)          as avg_close_price
from {{ ref('staging') }}
group by symbol
order by symbol

{{ config(
    materialized='table',
    unique_key='id'
) }}

with source_data as (
    select *
    from {{ source('dev', 'stock_data') }}
),

dedup as (
    select
        *,
        row_number() over (
            partition by symbol, timestamp
            order by updated_at desc
        ) as rn
    from source_data
)

select
    id,
    symbol,
    timestamp,
    interval,
    source,
    open,
    high,
    low,
    close,
    volume,
    created_at,
    updated_at
from dedup
where rn = 1

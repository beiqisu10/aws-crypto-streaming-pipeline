{{ config(materialized='view') }}

with raw_data as (
    select * from {{ source('crypto_raw', 'crypto_stats') }}
)

select
    window_start::timestamp as window_start_at,
    window_end::timestamp   as window_end_at,
    symbol                  as ticker,
    cast(avg_price as decimal(18,2))        as price,
    cast(total_volume as decimal(18,4))     as volume,
    cast(max_single_trade as decimal(18,4)) as max_trade,
    cast(trade_count as int)                as trade_count
from raw_data
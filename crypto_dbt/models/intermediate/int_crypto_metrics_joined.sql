{{ config(
    materialized='incremental', 
    unique_key=['window_start_at', 'ticker'] 
) }}

-- depends_on: {{ ref('stg_crypto_trades') }}

with base_trades as (
    select * from {{ ref('stg_crypto_trades') }}
),

metrics_calculation as (
    select
        *,
        -- Calculate Total Notional Value (Price * Volume)
        (price * volume) as total_value,
        -- Retrieve price from the previous window to calculate price delta
        lag(price) over (partition by ticker order by window_start_at) as prev_price
    from base_trades
)

select
    window_start_at,
    window_end_at,
    ticker,
    price,
    volume,
    max_trade,
    trade_count,
    total_value,
    -- Calculate price change percentage relative to the previous window
    case 
        when prev_price is null then 0 
        else ((price - prev_price) / prev_price) * 100 
    end as price_change_pct,
    -- Flag High-Frequency Trading (HFT) activity (e.g., > 100 trades per minute)
    case 
        when trade_count > 100 then true 
        else false 
    end as is_high_frequency
from metrics_calculation
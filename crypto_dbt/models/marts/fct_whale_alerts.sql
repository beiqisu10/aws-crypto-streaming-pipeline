{{
  config(
    materialized='incremental',
    unique_key='alert_id',
    incremental_strategy='delete+insert'
  )
}}

with enriched_metrics as (
    select * from {{ ref('int_crypto_metrics_joined') }}
    
    {% if is_incremental() %}
      -- Filter to only include data newer than the current maximum timestamp in the target table
      where window_start_at > (select max(window_start_at) from {{ this }})
    {% endif %}
)

select
    -- Generate a unique hash for each alert using ticker and timestamp
    md5(ticker || window_start_at::text) as alert_id,
    window_start_at,
    ticker,
    price,
    price_change_pct,
    total_value,
    trade_count,
    max_trade,
    -- Core Business Logic: Categorize traders based on transaction volume (Whale Detection)
    case 
        when max_trade >= 1.0 then 'MEGA WHALE'
        when max_trade >= 0.1 then 'WHALE'
        else 'RETAIL'
    end as trader_category,
    -- Advanced Business Logic: Generate market signals based on price volatility and activity
    case 
        when abs(price_change_pct) > 2.0 and max_trade >= 0.1 then 'VOLATILE WHALE MOVE'
        when trade_count > 100 then 'HIGH ACTIVITY'
        else 'NORMAL'
    end as market_signal
from enriched_metrics
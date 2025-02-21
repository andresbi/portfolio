
-- Fixture for stg_daily_bars
select 
    
        try_cast('2020-11-20' as DATE)
     as date, 
    
        try_cast('\'L\'' as character varying(16777216))
     as ticker, 
    
        try_cast('100' as FLOAT)
     as number_of_shares_traded, 
    
        try_cast('10' as NUMBER(38,0))
     as number_of_trades, try_cast(null as FLOAT) as volume_weighted_avg_price, try_cast(null as FLOAT) as open_price, try_cast(null as FLOAT) as close_price, try_cast(null as FLOAT) as high_price, try_cast(null as FLOAT) as low_price
union all
select 
    
        try_cast('2020-11-20' as DATE)
     as date, 
    
        try_cast('\'S\'' as character varying(16777216))
     as ticker, 
    
        try_cast('200' as FLOAT)
     as number_of_shares_traded, 
    
        try_cast('20' as NUMBER(38,0))
     as number_of_trades, try_cast(null as FLOAT) as volume_weighted_avg_price, try_cast(null as FLOAT) as open_price, try_cast(null as FLOAT) as close_price, try_cast(null as FLOAT) as high_price, try_cast(null as FLOAT) as low_price
union all
select 
    
        try_cast('2020-11-21' as DATE)
     as date, 
    
        try_cast('\'L\'' as character varying(16777216))
     as ticker, 
    
        try_cast('300' as FLOAT)
     as number_of_shares_traded, 
    
        try_cast('40' as NUMBER(38,0))
     as number_of_trades, try_cast(null as FLOAT) as volume_weighted_avg_price, try_cast(null as FLOAT) as open_price, try_cast(null as FLOAT) as close_price, try_cast(null as FLOAT) as high_price, try_cast(null as FLOAT) as low_price
union all
select 
    
        try_cast('2020-11-21' as DATE)
     as date, 
    
        try_cast('\'S\'' as character varying(16777216))
     as ticker, 
    
        try_cast('200' as FLOAT)
     as number_of_shares_traded, 
    
        try_cast('50' as NUMBER(38,0))
     as number_of_trades, try_cast(null as FLOAT) as volume_weighted_avg_price, try_cast(null as FLOAT) as open_price, try_cast(null as FLOAT) as close_price, try_cast(null as FLOAT) as high_price, try_cast(null as FLOAT) as low_price
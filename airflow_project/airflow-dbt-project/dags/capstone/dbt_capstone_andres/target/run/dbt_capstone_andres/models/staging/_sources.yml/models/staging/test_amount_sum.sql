-- Build actual result given inputs
with dbt_internal_unit_test_actual as (
  select
    week_start_date,number_of_shares_traded,number_of_trades,dollars_collected_by_treasury,irs_tax_dollars_collected_by_treasury, 'actual' as "actual_or_expected"
  from (
    with  __dbt__cte__stg_daily_bars as (

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
),  __dbt__cte__dim_date as (

-- Fixture for dim_date
select 
    
        try_cast('2020-11-20' as DATE)
     as date_day, try_cast(null as DATE) as prior_date_day, try_cast(null as DATE) as next_date_day, try_cast(null as DATE) as prior_year_date_day, try_cast(null as DATE) as prior_year_over_year_date_day, try_cast(null as NUMBER(3,0)) as day_of_week, try_cast(null as NUMBER(2,0)) as day_of_week_iso, try_cast(null as character varying(16777216)) as day_of_week_name, try_cast(null as character varying(3)) as day_of_week_name_short, try_cast(null as NUMBER(2,0)) as day_of_month, try_cast(null as NUMBER(4,0)) as day_of_year, 
    
        try_cast(' 2020-11-15' as DATE)
     as week_start_date, try_cast(null as DATE) as week_end_date, try_cast(null as DATE) as prior_year_week_start_date, try_cast(null as DATE) as prior_year_week_end_date, try_cast(null as NUMBER(38,0)) as week_of_year, try_cast(null as DATE) as iso_week_start_date, try_cast(null as DATE) as iso_week_end_date, try_cast(null as DATE) as prior_year_iso_week_start_date, try_cast(null as DATE) as prior_year_iso_week_end_date, try_cast(null as NUMBER(38,0)) as iso_week_of_year, try_cast(null as NUMBER(38,0)) as prior_year_week_of_year, try_cast(null as NUMBER(38,0)) as prior_year_iso_week_of_year, try_cast(null as NUMBER(38,0)) as month_of_year, try_cast(null as character varying(16777216)) as month_name, try_cast(null as character varying(16777216)) as month_name_short, try_cast(null as DATE) as month_start_date, try_cast(null as DATE) as month_end_date, try_cast(null as DATE) as prior_year_month_start_date, try_cast(null as DATE) as prior_year_month_end_date, try_cast(null as NUMBER(38,0)) as quarter_of_year, try_cast(null as DATE) as quarter_start_date, try_cast(null as DATE) as quarter_end_date, try_cast(null as NUMBER(38,0)) as year_number, try_cast(null as DATE) as year_start_date, try_cast(null as DATE) as year_end_date
union all
select 
    
        try_cast('2020-11-21' as DATE)
     as date_day, try_cast(null as DATE) as prior_date_day, try_cast(null as DATE) as next_date_day, try_cast(null as DATE) as prior_year_date_day, try_cast(null as DATE) as prior_year_over_year_date_day, try_cast(null as NUMBER(3,0)) as day_of_week, try_cast(null as NUMBER(2,0)) as day_of_week_iso, try_cast(null as character varying(16777216)) as day_of_week_name, try_cast(null as character varying(3)) as day_of_week_name_short, try_cast(null as NUMBER(2,0)) as day_of_month, try_cast(null as NUMBER(4,0)) as day_of_year, 
    
        try_cast(' 2020-11-15' as DATE)
     as week_start_date, try_cast(null as DATE) as week_end_date, try_cast(null as DATE) as prior_year_week_start_date, try_cast(null as DATE) as prior_year_week_end_date, try_cast(null as NUMBER(38,0)) as week_of_year, try_cast(null as DATE) as iso_week_start_date, try_cast(null as DATE) as iso_week_end_date, try_cast(null as DATE) as prior_year_iso_week_start_date, try_cast(null as DATE) as prior_year_iso_week_end_date, try_cast(null as NUMBER(38,0)) as iso_week_of_year, try_cast(null as NUMBER(38,0)) as prior_year_week_of_year, try_cast(null as NUMBER(38,0)) as prior_year_iso_week_of_year, try_cast(null as NUMBER(38,0)) as month_of_year, try_cast(null as character varying(16777216)) as month_name, try_cast(null as character varying(16777216)) as month_name_short, try_cast(null as DATE) as month_start_date, try_cast(null as DATE) as month_end_date, try_cast(null as DATE) as prior_year_month_start_date, try_cast(null as DATE) as prior_year_month_end_date, try_cast(null as NUMBER(38,0)) as quarter_of_year, try_cast(null as DATE) as quarter_start_date, try_cast(null as DATE) as quarter_end_date, try_cast(null as NUMBER(38,0)) as year_number, try_cast(null as DATE) as year_start_date, try_cast(null as DATE) as year_end_date
),  __dbt__cte__stg_treasury_revenue_collections as (

-- Fixture for stg_treasury_revenue_collections
select 
    
        try_cast('2020-11-20' as DATE)
     as collection_date, try_cast(null as character varying(16777216)) as channel_type, 
    
        try_cast('IRS Tax' as character varying(16777216))
     as tax_category, 
    
        try_cast('1000' as NUMBER(19,0))
     as dollars_collected_by_treasury
union all
select 
    
        try_cast('2020-11-20' as DATE)
     as collection_date, try_cast(null as character varying(16777216)) as channel_type, 
    
        try_cast('IRS Non Tax' as character varying(16777216))
     as tax_category, 
    
        try_cast('2000' as NUMBER(19,0))
     as dollars_collected_by_treasury
union all
select 
    
        try_cast('2020-11-21' as DATE)
     as collection_date, try_cast(null as character varying(16777216)) as channel_type, 
    
        try_cast('IRS Tax' as character varying(16777216))
     as tax_category, 
    
        try_cast('3000' as NUMBER(19,0))
     as dollars_collected_by_treasury
union all
select 
    
        try_cast('2020-11-21' as DATE)
     as collection_date, try_cast(null as character varying(16777216)) as channel_type, 
    
        try_cast('IRS Tax' as character varying(16777216))
     as tax_category, 
    
        try_cast('2000' as NUMBER(19,0))
     as dollars_collected_by_treasury
), pol as (
SELECT week_start_date, 
sum(number_of_shares_traded) number_of_shares_traded,
sum(number_of_trades) number_of_trades
from  __dbt__cte__stg_daily_bars a 
JOIN __dbt__cte__dim_date d on d.date_day=a.date
group by 1)

, rev as (
select week_start_date,
sum(dollars_collected_by_treasury) dollars_collected_by_treasury,
sum(case when tax_category = 'IRS Tax' then dollars_collected_by_treasury else 0 end) as irs_tax_dollars_collected_by_treasury, 
sum(case when tax_category = 'Non-Tax' then dollars_collected_by_treasury else 0 end) as non_irs_non_tax_dollars_collected_by_treasury,
sum(case when tax_category = 'IRS Non-Tax' then dollars_collected_by_treasury else 0 end) as irs_non_tax_dollars_collected_by_treasury
FROM __dbt__cte__stg_treasury_revenue_collections a 
JOIN __dbt__cte__dim_date d on d.date_day=a.collection_date
GROUP BY ALL)

select 
    a.week_start_date, 
    a.number_of_shares_traded, 
    a.number_of_trades,
    b.dollars_collected_by_treasury,
    b.irs_tax_dollars_collected_by_treasury

from pol a 
INNER JOIN rev b on b.week_start_date = a.week_start_date
  ) _dbt_internal_unit_test_actual
),
-- Build expected result
dbt_internal_unit_test_expected as (
  select
    week_start_date, number_of_shares_traded, number_of_trades, dollars_collected_by_treasury, irs_tax_dollars_collected_by_treasury, 'expected' as "actual_or_expected"
  from (
    select 
    
        try_cast('2020-11-15' as DATE)
     as week_start_date, 
    
        try_cast('800' as FLOAT)
     as number_of_shares_traded, 
    
        try_cast('120' as NUMBER(38,0))
     as number_of_trades, 
    
        try_cast('8000' as NUMBER(31,0))
     as dollars_collected_by_treasury, 
    
        try_cast('6000' as NUMBER(31,0))
     as irs_tax_dollars_collected_by_treasury
  ) _dbt_internal_unit_test_expected
)
-- Union actual and expected results
select * from dbt_internal_unit_test_actual
union all
select * from dbt_internal_unit_test_expected
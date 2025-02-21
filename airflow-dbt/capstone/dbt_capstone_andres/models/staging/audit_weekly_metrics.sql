with pol as (
SELECT week_start_date, 
sum(number_of_shares_traded) number_of_shares_traded,
sum(number_of_trades) number_of_trades
from  {{ref('stg_daily_bars')}} a 
JOIN {{ref('dim_date')}} d on d.date_day=a.date
group by 1)

, rev as (
select week_start_date,
sum(dollars_collected_by_treasury) dollars_collected_by_treasury,
sum(case when tax_category = 'IRS Tax' then dollars_collected_by_treasury else 0 end) as irs_tax_dollars_collected_by_treasury, 
sum(case when tax_category = 'Non-Tax' then dollars_collected_by_treasury else 0 end) as non_irs_non_tax_dollars_collected_by_treasury,
sum(case when tax_category = 'IRS Non-Tax' then dollars_collected_by_treasury else 0 end) as irs_non_tax_dollars_collected_by_treasury
FROM {{ref('stg_treasury_revenue_collections')}} a 
JOIN {{ref('dim_date')}} d on d.date_day=a.collection_date
GROUP BY ALL)

select 
    a.week_start_date, 
    a.number_of_shares_traded, 
    a.number_of_trades,
    b.dollars_collected_by_treasury,
    b.irs_tax_dollars_collected_by_treasury

from pol a 
INNER JOIN rev b on b.week_start_date = a.week_start_date

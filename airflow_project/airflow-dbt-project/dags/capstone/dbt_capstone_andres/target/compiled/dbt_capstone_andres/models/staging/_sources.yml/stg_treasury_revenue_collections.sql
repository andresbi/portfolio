
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
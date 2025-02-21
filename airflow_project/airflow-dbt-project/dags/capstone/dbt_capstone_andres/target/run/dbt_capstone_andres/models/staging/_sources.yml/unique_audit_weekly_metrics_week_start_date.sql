select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    week_start_date as unique_field,
    count(*) as n_records

from DATAEXPERT_STUDENT.andres.audit_weekly_metrics
where week_start_date is not null
group by week_start_date
having count(*) > 1



      
    ) dbt_internal_test
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select week_start_date
from DATAEXPERT_STUDENT.andres.audit_weekly_metrics
where week_start_date is null



      
    ) dbt_internal_test

      
  
    

        create or replace transient table dataexpert_student.andres.snapshot_customers
         as
        (
    

    select *,
        md5(coalesce(cast(id as varchar ), '')
         || '|' || coalesce(cast(last_updated_dt as varchar ), '')
        ) as dbt_scd_id,
        last_updated_dt as dbt_updated_at,
        last_updated_dt as dbt_valid_from,
        
  
  coalesce(nullif(last_updated_dt, last_updated_dt), null)
  as dbt_valid_to

    from (
        



select * from dataexpert_student.bootcamp.js_raw_customers

    ) sbq



        );
      
  
  
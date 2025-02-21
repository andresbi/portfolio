
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from DATAEXPERT_STUDENT.andres_79435.stg_orders
    group by status

)

select *
from all_values
where value_field not in (
    'return_pending','returned','completed','placed','shipped'
)



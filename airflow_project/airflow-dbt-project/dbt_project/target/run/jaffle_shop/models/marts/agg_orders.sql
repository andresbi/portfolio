
  
    

        create or replace transient table DATAEXPERT_STUDENT.andres.agg_orders
         as
        (with
    fact_orders as (
        select
            order_date
            , status
            , amount
        from DATAEXPERT_STUDENT.andres.fact_orders
    )

    , aggregated as (
        select
            order_date,
             status
            , sum(amount) as total_amount
        from fact_orders
        group by
            order_date
            , status
    )

select *
from aggregated
        );
      
  
with source as (

    select * from dataexpert_student.andres.fiscal_data

),

renamed as (

    select
        record_date as collection_date,
        channel_type_desc as channel_type,
        tax_category_desc as tax_category,
        round(net_collections_amt,0) as dollars_collected_by_treasury

    from source

)

select * from renamed
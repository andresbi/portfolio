with source as (

    select * from dataexpert_student.andres.daily_bars

),

renamed as (

    select
        TO_DATE(TO_TIMESTAMP(timestamp / 1000)) as date,
        ticker as ticker,
        v  as number_of_shares_traded,
        n  as number_of_trades,
        round(vw,2) as volume_weighted_avg_price,
        o  as open_price,
        c  as close_price,
        h  as high_price,
        l  as low_price
    from source

)

select * from renamed
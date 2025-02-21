

{{
    config(
        materialized='incremental',
        unique_key='week_start_date',
        incremental_strategy = 'merge'
    )
}}

SELECT *
from  {{ref('audit_weekly_metrics')}} 

{% if is_incremental() %}

where week_start_date >= (select coalesce(max(week_start_date)-7,'1900-01-01') from {{ this }} )

{% endif %}
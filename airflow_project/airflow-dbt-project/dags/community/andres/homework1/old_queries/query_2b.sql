WITH
  base AS (
    SELECT DISTINCT
      actor_id,
      YEAR
    FROM
      bootcamp.actor_films
    WHERE TRUE 
   -- AND actor_id = 'nm0000273'
    AND year < (select max(year) from bootcamp.actor_films )
    order by 1,2
  )
  
  , min_max_by_actor AS (
    SELECT
      actor_id,
      min(YEAR) min_yr,
      max(YEAR) max_yr
    FROM
      base
    GROUP BY
      1
  )
  
  ,span AS (
    SELECT
      YEAR
    FROM
      UNNEST (
        sequence(
          (
            SELECT
              min(YEAR) year_span
            FROM
              base
          ),
          (
            SELECT
              max(YEAR) year_span
            FROM
              base
          ),
          1
        )
      ) AS t (YEAR)
  )
    
  ,
  base_x AS (
    SELECT DISTINCT
      b.actor_id,
      s.year
    FROM
      span s,
      base b
  )
    ,
  resolved AS (
    SELECT
      x.actor_id,
      x.year,
      CASE
        WHEN b.year IS NOT NULL THEN 1
        ELSE 0
      END AS is_active
    FROM
      base_x x
      LEFT JOIN base b ON b.actor_id = x.actor_id
      AND x.year = b.year
      INNER JOIN min_max_by_actor mm ON mm.actor_id = x.actor_id
      AND x.year >= mm.min_yr
      AND x.year <= mm.max_yr
  )
  ,
  resolved2 AS (
    SELECT
      actor_id,
      YEAR,
      is_active,
      coalesce(
        lag(is_active) OVER (
          PARTITION BY
            actor_id
          ORDER BY
            YEAR
        ),
        1
      ) AS active_last_year
    FROM
      resolved
    ORDER BY
      actor_id,
      YEAR
  )
    ,
  resolved3 AS (
    SELECT
      *,
      sum(
        CASE
          WHEN is_active <> active_last_year THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          actor_id
        ORDER BY
          YEAR
      ) AS streak
    FROM
      resolved2
  )
  
  , final_scd as (
    select 
    actor_id, 
    case when is_active = 1 then TRUE else FALSE end is_active,
    min(year) over (partition by actor_id, streak)   start_date,
   year   as end_date,
    year
    
     from resolved3 
     order by year
)

, scd as (
select * from final_scd
)

, current_season as (
SELECT actor_id, year FROM bootcamp.actor_films
where year = (select max(year) from bootcamp.actor_films)
--and actor_id = 'nm0000273'
order by 2
)

select 
coalesce(s.actor_id, b.actor_id) actor_id,
coalesce(s.is_active, TRUE) is_active,
coalesce(s.start_date,b.year) start_date,
coalesce(s.end_date,b.year) end_date,
coalesce(s.year,b.year) year
from scd s
full outer join current_season b on b.actor_id = s.actor_id and s.year+1 = b.year
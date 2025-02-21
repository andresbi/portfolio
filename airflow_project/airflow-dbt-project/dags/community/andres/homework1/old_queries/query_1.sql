WITH
  base AS (
    SELECT DISTINCT
      actor_id,
      YEAR
    FROM
      bootcamp.actor_films
      --WHERE actor_id = 'nm0000006'
  ),
  min_max_by_actor AS (
    SELECT
      actor_id,
      min(YEAR) min_yr,
      max(YEAR) max_yr
    FROM
      base
    GROUP BY
      1
  ),
  span AS (
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
  ),
  base_x AS (
    SELECT DISTINCT
      b.actor_id,
      s.year
    FROM
      span s,
      base b
  ),
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
  ),
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
  ),
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
  ),
  resolved4 AS (
    SELECT
      actor_id,
      max(is_active) = 1 is_active,
      min(YEAR) AS year_from,
      max(YEAR) AS year_to,
      2021 AS current_year
    FROM
      resolved3
    GROUP BY
      actor_id,
      streak
    ORDER BY
      actor_id,
      3 DESC
  )
SELECT
  actor_id,
  is_active,
  CAST(date_parse(CAST(year_from AS VARCHAR) || '-01-01', '%Y-%m-%d') AS DATE) AS start_date,
  CAST(date_parse(CAST(  year_to AS VARCHAR) || '-12-31', '%Y-%m-%d') AS DATE) AS end_date,
  current_year
FROM
  resolved4
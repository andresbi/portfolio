create or replace table andres_79435.actors_history_scd
(
 actor_id VARCHAR,
 actor VARCHAR,
 is_active BOOLEAN,
 start_date INT,
 end_date INT,
 year INT
 )
 WITH (
 partitioning = ARRAY['year']
)
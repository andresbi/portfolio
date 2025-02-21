DELETE FROM andres_79435.actors_history_scd

drop table andres_79435.actors_history_scd


create table andres_79435.mini_actors


select min(year), max(year) from andres_79435.mini_actors


CREATE OR REPLACE TABLE andres_79435.mini_actors (

                actor_id VARCHAR,
                actor VARCHAR,
                is_active BOOLEAN,
                year INTEGER
             ) 

             WITH (
                partitioning = ARRAY['year']
             )

insert into andres_79435.mini_actors
select actor_id, actor, true as is_active, year from bootcamp.actor_films where year < 1926

select * from andres_79435.actors_history_scd ORDER BY YEAR DESC
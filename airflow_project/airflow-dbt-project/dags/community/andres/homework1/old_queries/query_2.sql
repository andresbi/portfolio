
with last_season_scd as (
select * from andres_79435.actors_history_scd
where actor_id = 'nm0005207')

, current_season as (
SELECT * FROM bootcamp.actor_films
where year = 2021 and actor_id = 'nm0005207'
)

, update_record as (

SELECT 
coalesce(ls.actor_id, cs.actor_id) actor_id,
ls.is_active,
coalesce(ls.year_from, cs.year) year_from,
coalesce(ls.year_to, cs.year) year_to,
cs.year,
case 
  when ls.is_active = FALSE and cs.year is not null
  then FALSE
   --means that the last record was inactive but a movie came up so now we have to add a new record to show that its active. Value is false because it does not need to change 
  when ls.is_active = FALSE and cs.year is null
  then FALSE 
  when ls.is_active = TRUE and cs.year is not null then TRUE --means that it was active and a movie came up so i it remains active, but now we need to adjust the year
  when ls.is_active = TRUE and cs.year is null
  then FALSE 

end change_record
FROM last_season_scd ls
FULL OUTER JOIN current_season cs 
  ON ls.actor_id = cs.actor_id AND 
     ls.year_from + 1 = cs.year
)

, updates_included as (
select 
actor_id,
is_active,
year_from,
--year_to,
case when change_record then year 
else year_to
end as year_to
 from update_record
 order by year_from)
 
 , unioned as (
 select * from updates_included 
 union
 select 
actor_id,
TRUE as is_active,
year,
year
 from current_season)
 
 select * from unioned 
 order by actor_id, year_from

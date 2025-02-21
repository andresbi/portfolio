insert into andres_79435.actors_history_scd

with yesterday as (
  select
    *
  from
    andres_79435.actors_history_scd
  where
    year = 1917
 --   and actor = 'Lillian Gish'
),
 today as (
   select
     actor,
     actor_id,
     is_active,
     max(year) year
    from
      andres_79435.mini_actors
    where year = 1918
   -- and actor = 'Lillian Gish'
   group by
    actor,
    actor_id,
    is_active
)

,
 actor_status as (
    select
     coalesce(y.actor_id, t.actor_id) as actor_id,
     coalesce(y.actor, t.actor) as actor,
     t.year is not null as is_active,
     case
      when coalesce(y.is_active,FALSE) <> coalesce(t.is_active,FALSE) then 1
      when coalesce(y.is_active,FALSE) = coalesce(t.is_active,FALSE) then 0
    end as rec_change, 
     coalesce(y.start_date, t.year) as start_date,
     coalesce(y.end_date, t.year) as end_date,
     coalesce(t.year, y.year+1) as year
   from
     yesterday y full join today t on y.actor_id = t.actor_id
)


, 
update_status as (
 select
  actor_id,
  actor,
  is_active,
  case 
    when rec_change = 0 or rec_change is null or start_date = year then start_date 
    else start_date+1 end as start_date,
  case
    when rec_change = 1 then end_date
    when rec_change = 0 then end_date + 1
    else year end as end_date,
  year
from
  actor_status
)

select
  actor_id,
  actor,
  is_active,
  start_date,
  end_date,
  year
from
 update_status

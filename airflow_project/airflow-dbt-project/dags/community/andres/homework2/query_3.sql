
with base as (
select gd.player_name as player, coalesce(gd.team_abbreviation,'(overall)') as team,
coalesce(g.season,0) as season, 
sum(gd.reb) rebounds, sum(ast) assists, sum(pts) points 
from bootcamp.nba_game_details gd
JOIN bootcamp.nba_games g on g.game_id = gd.game_id 

group by grouping sets (
  (gd.player_name),
  (gd.player_name,gd.team_abbreviation),
  (gd.player_name,g.season)
))

select player from (
select * , dense_rank() over (order by points desc) ranking
from base  
where team <> '(overall)' and season = 0 ) as a 
where ranking = 1
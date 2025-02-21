select gd.player_name as player, coalesce(gd.team_abbreviation,'(overall)') as team,
coalesce(g.season,0) as season, 
sum(gd.reb) rebounds, sum(ast) assists, sum(pts) points 
from bootcamp.nba_game_details gd
JOIN bootcamp.nba_games g on g.game_id = gd.game_id 
group by grouping sets (
  (gd.player_name),
  (gd.player_name,gd.team_abbreviation),
  (gd.player_name,g.season)
)
order by gd.player_name, gd.team_abbreviation
SELECT DISTINCT
  coalesce(ts.player_name,ls.player_name) player, 
  ts.current_season season,
  ts.is_active,
  case 
    when ts.is_active      and ls.is_active is null then 'New'
    when ts.is_active      and ls.is_active         then 'Continued Playing'
    when not(ts.is_active) and ls.is_active         then 'Retired'
    when not(ts.is_active) and not(ls.is_active)    then 'Stayed Retired'
    when ts.is_active      and not(ls.is_active)    then 'Returned from Retirement'
  end change_tracking
FROM 
bootcamp.nba_players ts
FULL JOIN bootcamp.nba_players ls on ls.player_name    = ts.player_name and 
                                     ls.current_season = ts.current_season - 1
WHERE ts.current_season is not null  
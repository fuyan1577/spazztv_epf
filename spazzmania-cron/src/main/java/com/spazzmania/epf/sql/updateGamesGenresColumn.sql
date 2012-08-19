-- EPF Application to Spazzmania Update Script
-- Update the Games Genres column as a comma separated list of Genre IDs
-- Copyright 2011 Spazzmania, Inc
SET AUTOCOMMIT=1;
-- LOCK TABLES spazzmania.games g write,
-- spazzmania.games_genres gg read,
-- spazzmania.genres gr read;
update spazzmania.games g
set g.genres = (
	select cast(group_concat(gg.genre_id) as char)
	from spazzmania.games_genres gg, 
	spazzmania.genres gr
	where gg.game_id = g.id
	and gr.id = gg.genre_id
	and gr.parent_id = 6014
	group by gg.game_id);

update spazzmania.games g
set g.genres = '6014'
    where g.genres = '';
	
COMMIT;
-- UNLOCK TABLES;


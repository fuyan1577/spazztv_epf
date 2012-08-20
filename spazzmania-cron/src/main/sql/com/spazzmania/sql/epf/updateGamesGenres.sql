-- EPF Application to Spazzmania Update Script
-- Update games genres from epf.genre_application
-- Copyright 2011 Spazzmania, Inc
SET AUTOCOMMIT=1;
-- LOCK TABLES spazzmania.games_genres  write,
-- epf.genre_application ga read,
-- spazzmania.games g read;
REPLACE INTO spazzmania.games_genres
	(game_id,genre_id,is_primary,created,modified) 
SELECT g.id,ga.genre_id,ga.is_primary,from_unixtime(ga.export_date / 1000),
	from_unixtime(ga.export_date / 1000)
FROM epf.genre_application ga 
INNER JOIN spazzmania.games g 
   ON (ga.application_id = g.os_game_id and g.os_game='ios');
COMMIT;
-- UNLOCK TABLES;

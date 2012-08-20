-- EPF Application to Spazzmania Update Script
-- Update Custom Genre: Shooting
-- Copyright 2011 Spazzmania, Inc
SET AUTOCOMMIT=1;
-- LOCK TABLES spazzmania.games_genres write,
-- spazzmania.games read;
INSERT IGNORE INTO spazzmania.games_genres(game_id , genre_id,created)
	select games.id,97008,NOW() from spazzmania.games WHERE
	upper(games.description) like ('%SHOOTER%') 
	or upper(games.description) like ('%FPS%');
COMMIT;
-- UNLOCK TABLES;

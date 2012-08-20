-- EPF Application to Spazzmania Update Script
-- Update Custom Genre: Retro
-- Copyright 2011 Spazzmania, Inc
SET AUTOCOMMIT=1;
-- LOCK TABLES spazzmania.games_genres write,
-- spazzmania.games read;
INSERT IGNORE INTO spazzmania.games_genres(game_id , genre_id,created)
	select games.id,97007,NOW() from spazzmania.games WHERE
	upper(games.title) like '%RETRO%' or upper(games.description) like '%RETRO%';
COMMIT;
-- UNLOCK TABLES;

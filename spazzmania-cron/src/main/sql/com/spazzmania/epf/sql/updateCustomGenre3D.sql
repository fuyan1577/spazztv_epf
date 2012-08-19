-- EPF Application to Spazzmania Update Script
-- Update Custom Genre: Drinking
-- Copyright 2011 Spazzmania, Inc
SET AUTOCOMMIT=1;
-- LOCK TABLES spazzmania.games_genres write,
-- spazzmania.games read;
INSERT IGNORE INTO spazzmania.games_genres(game_id , genre_id,created)
	SELECT games.id, 97001, NOW() from spazzmania.games WHERE
	upper(games.title) like ('%3D%');
COMMIT;
-- UNLOCK TABLES;

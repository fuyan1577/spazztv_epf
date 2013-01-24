-- EPF Application to Spazzmania Update Script
-- Update Custom Genre: Multiplayer
-- Copyright 2011 Spazzmania, Inc
SET AUTOCOMMIT=1;
-- LOCK TABLES spazzmania.games_genres write,
-- spazzmania.games read,
-- spazzmania.games_genres gg read;
INSERT IGNORE INTO spazzmania.games_genres(game_id , genre_id,created)
	SELECT games.id, 97006, NOW() from spazzmania.games
	INNER JOIN spazzmania.games_genres gg 
	  ON gg.game_id = games.id AND gg.genre_id = 97005
	WHERE
	upper(games.title) like ('%MULTIPLAYER%') 
	OR upper(games.description) like ('%MULTIPLAYER%');
COMMIT;
-- UNLOCK TABLES;

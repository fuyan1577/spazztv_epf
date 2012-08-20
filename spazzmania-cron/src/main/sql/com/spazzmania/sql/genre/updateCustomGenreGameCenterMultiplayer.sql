-- EPF Application to Spazzmania Update Script
-- Update Custom Genre: Game Center Multiplayer
-- Copyright 2011 Spazzmania, Inc
SET AUTOCOMMIT=1;
-- LOCK TABLES spazzmania.games_genres write,
-- spazzmania.games read;
INSERT IGNORE INTO spazzmania.games_genres(game_id , genre_id,created)
	SELECT games.id, 97006, NOW() from spazzmania.games WHERE
	(upper(games.title) like ('%GAME CENTER%') 
		and upper(games.title) like ('%MULTIPLAYER%'))
    OR 
	(upper(games.description) like ('%GAME CENTER%') 
        and upper(games.description) like ('%MULTIPLAYER%'));
COMMIT;
-- UNLOCK TABLES;

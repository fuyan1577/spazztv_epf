-- EPF Application to Spazzmania Update Script
-- Update Custom Genre: Best 99 Cent
-- Copyright 2011 Spazzmania, Inc
SET AUTOCOMMIT=1;
-- LOCK TABLES spazzmania.games_genres write,
-- spazzmania.games g read,
-- spazzmania.game_prices gp read,
-- spazzmania.games_genres gg read;
INSERT IGNORE INTO spazzmania.games_genres(game_id , genre_id,created)
	SELECT g.id, 97010, NOW()
		FROM spazzmania.games g
		INNER JOIN spazzmania.game_prices gp ON ( gp.game_id = g.id
		AND gp.storefront_id =143441 )
		INNER JOIN spazzmania.games_genres gg ON ( gg.game_id = g.id
		AND gg.genre_id =6014 )
		WHERE gp.price = 0.99;
COMMIT;
-- UNLOCK TABLES;

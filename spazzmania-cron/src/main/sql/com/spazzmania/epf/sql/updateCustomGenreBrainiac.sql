-- EPF Application to Spazzmania Update Script
-- Update Custom Genre: Drinking
-- Copyright 2011 Spazzmania, Inc
SET AUTOCOMMIT=1;
INSERT IGNORE INTO spazzmania.games_genres(game_id , genre_id,created)
	SELECT games.id, 97011, NOW() from spazzmania.games WHERE
		description like '%brain teaser%'
		or description like '%brainiac%'
		or description like '%mind game%'
		or description like '%mental game%'
		or description like '%memory game%'
		or description like '%mental challenge%'
		or description like '%math game%'
		or description like '%math challenge%'
		or description like '%history game%'
		or description like '%history challenge%';
COMMIT;

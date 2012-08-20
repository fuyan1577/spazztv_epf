-- EPF Application to Spazzmania Update Script
-- Find epf.applications that have cheats in the title 
-- and port them over to Spazzmania
-- Copyright 2011 Spazzmania, Inc

CREATE TEMPORARY TABLE spazzmania.game_cheats LIKE spazzmania.games;

INSERT IGNORE INTO spazzmania.game_cheats
	(os_game,os_game_id,title,recommended_age,
	publisher_name,app_store_link,artwork_url_large,
	artwork_url_small,release_date,description,version,
	download_size,platform,status,update_date,modified,created) 
SELECT 'ios',a.application_id,a.title,a.recommended_age,
	a.seller_name,a.view_url,a.artwork_url_large,
	a.artwork_url_small,a.itunes_release_date,a.description,a.version,
	a.download_size,
	IF (ad.ipad_screenshot_url_1 is not null and ad.screenshot_url_1 is not null,'Universal',
		IF(ad.ipad_screenshot_url_1 is not null, 'iPad', 'iPhone')),
	IF(g.version = a.version AND g.id is NOT NULL, 'released', 'updated'),
	IF(g.version = a.version AND g.id is NOT NULL, g.update_date, from_unixtime(a.export_date/1000)),
	IF(g.version = a.version AND g.id is NOT NULL, g.modified, from_unixtime(a.export_date/1000)),
from_unixtime(a.export_date/1000)
from epf.application a
inner join epf.application_detail ad on (ad.application_id = a.application_id)
left join spazzmania.games g on (g.os_game_id = a.application_id and g.os_game = 'ios')
WHERE a.title LIKE '%cheats%'
and g.id is null;

REPLACE INTO spazzmania.games
	(os_game,os_game_id,title,recommended_age,
	publisher_name,app_store_link,artwork_url_large,
	artwork_url_small,release_date,description,version,
	download_size,platform,status,update_date,modified,created) 
SELECT os_game,os_game_id,title,recommended_age,
	publisher_name,app_store_link,artwork_url_large,
	artwork_url_small,release_date,description,version,
	download_size,platform,status,update_date,modified,created
FROM spazzmania.game_cheats;

DROP TABLE spazzmania.game_cheats;

-- Set the Cheats Genre for the games just added
INSERT INTO spazzmania.games_genres 
	(game_id,genre_id,is_primary,created,modified) 
SELECT g.id,97009,0,NOW(),NOW()
from spazzmania.games g 
LEFT JOIN spazzmania.games_genres ogg 
	on (ogg.game_id = g.id AND ogg.genre_id=97009)
WHERE g.title LIKE '%cheats%' AND ogg.id IS NULL;

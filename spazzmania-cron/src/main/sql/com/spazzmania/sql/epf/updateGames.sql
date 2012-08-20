-- EPF Application to Spazzmania Games Update Script
-- This script updates the spazzmania games table with any new or modified
-- games found on the epf.application table.
-- Copyright 2011 Spazzmania
SET AUTOCOMMIT=1;
-- Lock the tables for each table or alias used in the query
-- LOCK TABLES spazzmania.games g write,
-- 			spazzmania.games write,
-- 			epf.application a read,
-- 			epf.genre_application ga read,
-- 			epf.application_detail ad read;
INSERT INTO spazzmania.games
	(os_game,os_game_id,title,recommended_age,publisher_name,app_store_link,
	artwork_url_large,artwork_url_small,release_date,description,version,
	download_size,platform,status,update_date,modified,created) 
SELECT
	'ios',a.application_id,a.title,a.recommended_age,a.seller_name,a.view_url,
	a.artwork_url_large,a.artwork_url_small,a.itunes_release_date,a.description,
	a.version,a.download_size,
	IF (ad.ipad_screenshot_url_1 is not null and ad.screenshot_url_1 is not null,'Universal',
	                               IF(ad.ipad_screenshot_url_1 is not null, 'iPad', 'iPhone')),
	IF(g.version = a.version AND g.id is NOT NULL, 'released','updated'),
	IF(g.version = a.version AND g.id is NOT NULL,g.update_date, from_unixtime(a.export_date/1000)),
	IF(g.version = a.version AND g.id is NOT NULL, g.modified,from_unixtime(a.export_date/1000)),
	from_unixtime(a.export_date/1000)
from epf.application a
inner join epf.genre_application ga 
  on (ga.application_id = a.application_id and ga.genre_id = 6014 and ga.is_primary = 1)
inner join epf.application_detail ad 
  on (ad.application_id = a.application_id)
left join spazzmania.games g 
  on (g.os_game_id = a.application_id and g.os_game = 'ios')
ON duplicate KEY 
	UPDATE os_game = 'ios',
	os_game_id = a.application_id,
	title = a.title,
	recommended_age = a.recommended_age,
	publisher_name = a.seller_name,
	app_store_link = a.view_url,
	artwork_url_large = a.artwork_url_large,
	artwork_url_small = a.artwork_url_small,
	release_date = a.itunes_release_date,
	description = a.description,
	version = a.version,
	download_size = a.download_size,
	platform = IF (ad.ipad_screenshot_url_1 is not null and ad.screenshot_url_1 is not null,
		          'Universal',
		           IF(ad.ipad_screenshot_url_1 is not null, 'iPad', 'iPhone')),
	status = IF(g.version = a.version AND g.id is NOT NULL,'released', 'updated'),
	update_date = IF(g.version = a.version AND g.id is NOT NULL, 
		          g.update_date,
	              from_unixtime(a.export_date/1000)),
	modified = IF(g.version = a.version AND g.id is NOT NULL, 
		       g.modified,
	           from_unixtime(a.export_date/1000)),
	created = from_unixtime(a.export_date/1000);
COMMIT;
-- UNLOCK TABLES;


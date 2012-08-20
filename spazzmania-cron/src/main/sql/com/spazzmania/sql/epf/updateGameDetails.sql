-- EPF Application to Spazzmania Update Script
-- Update the game_details table from epf.application_detail
-- Copyright 2011 Spazzmania, Inc
SET AUTOCOMMIT=1;
-- LOCK TABLES spazzmania.game_details write,
-- spazzmania.games g read,
-- epf.application_detail ad read;
REPLACE INTO spazzmania.game_details
	(game_id,language_code,release_notes,screenshot_url_1,screenshot_url_2,
	screenshot_url_3,screenshot_url_4,ipad_screenshot_url_1,
	ipad_screenshot_url_2,ipad_screenshot_url_3,ipad_screenshot_url_4,
	screenshot_width_height_1,screenshot_width_height_2,screenshot_width_height_3,
	screenshot_width_height_4,ipad_screenshot_width_height_1,
	ipad_screenshot_width_height_2,ipad_screenshot_width_height_3,
	ipad_screenshot_width_height_4) 
SELECT g.id,ad.language_code,ad.release_notes,ad.screenshot_url_1,
	ad.screenshot_url_2,ad.screenshot_url_3,ad.screenshot_url_4,
	ad.ipad_screenshot_url_1,ad.ipad_screenshot_url_2,ad.ipad_screenshot_url_3,
	ad.ipad_screenshot_url_4,ad.screenshot_width_height_1,
	ad.screenshot_width_height_2,ad.screenshot_width_height_3,
	ad.screenshot_width_height_4,ad.ipad_screenshot_width_height_1,
	ad.ipad_screenshot_width_height_2,ad.ipad_screenshot_width_height_3,
	ad.ipad_screenshot_width_height_4
from spazzmania.games g
inner join epf.application_detail ad 
	on (ad.application_id = g.os_game_id and g.os_game='ios');
COMMIT;
-- UNLOCK TABLES;


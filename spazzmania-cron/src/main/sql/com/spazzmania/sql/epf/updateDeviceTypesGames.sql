-- EPF Application to Spazzmania Update Script
-- Update Device Type Games from epf.application_device_type
-- Copyright 2011 Spazzmania, Inc
SET AUTOCOMMIT=1;
-- LOCK TABLES spazzmania.device_types_games write,
-- spazzmania.games g read,
-- epf.application_device_type adt read;
REPLACE INTO spazzmania.device_types_games
	(game_id,device_type_id,created,modified) 
SELECT g.id,adt.device_type_id,from_unixtime(adt.export_date / 1000),
	from_unixtime(adt.export_date / 1000)
from spazzmania.games g
left join epf.application_device_type adt 
   on (adt.application_id = g.os_game_id and g.os_game='ios');
COMMIT;
-- UNLOCK TABLES;

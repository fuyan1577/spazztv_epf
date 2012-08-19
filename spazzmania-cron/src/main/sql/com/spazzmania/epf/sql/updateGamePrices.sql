-- EPF Application to Spazzmania Update Script
-- Update the game_prices table from epf.application_price
-- Copyright 2011 Spazzmania, Inc
SET AUTOCOMMIT=1;
-- LOCK TABLES spazzmania.game_prices write,
-- spazzmania.games g read,
-- epf.application_price ap read,
-- spazzmania.game_prices gp read;
INSERT INTO spazzmania.game_prices 
	(game_id,storefront_id,currency_code,price,price_date,previous_price,
	created,modified) 
SELECT g.id,ap.storefront_id,
	ap.currency_code,ap.retail_price,
	IF(gp.price <> ap.retail_price, from_unixtime(ap.export_date / 1000), gp.price_date),
	IF(gp.price <> ap.retail_price, gp.price, gp.previous_price),
	from_unixtime(ap.export_date / 1000),from_unixtime(ap.export_date / 1000)
from spazzmania.games g
left join epf.application_price ap 
	on (ap.application_id = g.os_game_id and g.os_game='ios')
left join spazzmania.game_prices gp 
	on (gp.game_id = g.id AND gp.storefront_id = ap.storefront_id)
ON duplicate KEY 
UPDATE game_id = g.id,
	storefront_id = ap.storefront_id,
	currency_code = ap.currency_code,
	price = ap.retail_price,
	price_date = IF(gp.price <> ap.retail_price, from_unixtime(ap.export_date / 1000), gp.price_date),
	previous_price = IF(gp.price <> ap.retail_price, gp.price, gp.previous_price),
	created = from_unixtime(ap.export_date / 1000),
	modified = from_unixtime(ap.export_date / 1000);
COMMIT;
-- UNLOCK TABLES;

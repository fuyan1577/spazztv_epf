-- EPF Application to Spazzmania Update Script
-- Update the Limited Time Offer indicater
-- Copyright 2011 Spazzmania, Inc
SET AUTOCOMMIT=1;
-- LOCK TABLES spazzmania.game_prices gp write,
-- spazzzmania.games g read;
UPDATE spazzmania.game_prices gp
left join spazzmania.games g on (g.id = gp.game_id)
SET gp.limited_time_offer = 1 
WHERE g.description like '%limited time%';
COMMIT;
-- UNLOCK TABLES;

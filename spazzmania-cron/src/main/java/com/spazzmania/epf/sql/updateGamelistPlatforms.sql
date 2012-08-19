-- EPF Application to Spazzmania Update Script
-- Update any gamelist games where the platform changed
-- Copyright 2011 Spazzmania, Inc
SET AUTOCOMMIT=1;
-- LOCK TABLES gamelists write, games read;
UPDATE gamelists, games 
set gamelists.platform = games.platform 
where gamelists.game_id = games.id 
  AND gamelists.platform <> games.platform;
COMMIT;
-- UNLOCK TABLES;

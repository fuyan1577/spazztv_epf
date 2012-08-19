-- EPF Application to Spazzmania Update Script
-- Update Master Tables from EPF
--	device_types
--  genres
--  storefronts
SET AUTOCOMMIT=1;
-- LOCK TABLES spazzmania.device_types write,
-- epf.device_type edt read;
insert IGNORE into spazzmania.device_types
	select edt.device_type_id as `id`,
	edt.name,
	edt.name as `epf_name`,
	now() as created,
	now() as modified
	from epf.device_type edt;
COMMIT;
-- UNLOCK TABLES;

SET AUTOCOMMIT=1;
-- LOCK TABLES spazzmania.genres write,
-- epf.genre eg read;
insert IGNORE into spazzmania.genres (id,parent_id,name,created,modified)
	select eg.genre_id as `id`,
	eg.parent_id,
	eg.name,
	now() as created,
	now() as modified
	from epf.genre eg
	where eg.parent_id in (36,6014);
COMMIT;
-- UNLOCK TABLES;

SET AUTOCOMMIT=1;
-- LOCK TABLES spazzmania.storefronts write,
-- epf.storefront es read;
insert IGNORE into spazzmania.storefronts
	select es.storefront_id as `id`,
	es.country_code,
	es.name,
	now() as created,
	now() as modified
	from epf.storefront es;
COMMIT;
-- UNLOCK TABLES;

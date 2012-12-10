#!/bin/sh
################################################################################
#SpazzTV EPF Importer
#Copyright SpazzTV, Inc. 2012
################################################################################

java com.spazztv.epf.EPFImporter -cp .;./lib;%CLASSPATH% \
	--db_config=config/EPFDbConfig.json \
	--config=config/EPFConfig.json

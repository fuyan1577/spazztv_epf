package com.spazzmania.epf.ingester;

import java.util.Map;

/**
 * @author Thomas Billingsley
 * @version 1.0
 */
public class EPFDbWriterFactory {
	
	private static EPFDbWriterFactory factory = null;
	private EPFDbConfig dbConfig;
	
	private EPFDbConnector connector;

	private EPFDbWriterFactory(EPFDbConfig dbConfig) {
		this.dbConfig = dbConfig;
		connector = new EPFDbConnector(dbConfig);
	}

	/**
	 * 
	 * @param dbConfig
	 */
	public static EPFDbWriter getDbWriter(EPFDbConfig dbConfig){
		if (factory == null) {
			factory = new EPFDbWriterFactory(dbConfig);
		}
		return factory.newDbWriterInstance(dbConfig);
	}
	
	private EPFDbWriter newDbWriterInstance(EPFDbConfig dbConfig) {
//		return new EPFDbWriter()
		return new EPFDbWriter(dbConfig.getTablePrefix())
		return null;
	}
	
	private EPFDbConnector getConnection(EPFDbConfig dbConfig) {
		EPFDbConnectorMap.getConnectorMap();
	}

}
package com.spazzmania.epf.ingester;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Thomas Billingsley
 * @version 1.0
 */
public class EPFDbWriterFactory {

	public static Map<String, String> EPF_DB_WRITER_MAP = Collections
			.unmodifiableMap(new HashMap<String, String>() {
				private static final long serialVersionUID = 1L;
				{
					put("com.jdbc.mysql.Driver",
							com.spazzmania.epf.ingester.EPFDbWriterMySQL.class
									.getName());
				}
			});

	private static EPFDbWriterFactory factory = null;

	private EPFDbConnector connector;

	private EPFDbWriterFactory(EPFDbConfig dbConfig) {
		connector = new EPFDbConnector(dbConfig);
	}

	/**
	 * 
	 * @param dbConfig
	 * @throws EPFDbException
	 */
	public static EPFDbWriter getDbWriter(EPFDbConfig dbConfig)
			throws EPFDbException {
		if (factory == null) {
			factory = new EPFDbWriterFactory(dbConfig);
		}
		return factory.newDbWriterInstance(dbConfig);
	}

	private EPFDbWriter newDbWriterInstance(EPFDbConfig dbConfig)
			throws EPFDbException {
		if (!EPF_DB_WRITER_MAP.containsKey(dbConfig.getDbDriverClass())) {
			throw new EPFDbException("Unknown/Unsupported Driver: "
					+ dbConfig.getDbDriverClass());
		}

		EPFDbWriter dbWriter = null;

		try {
			dbWriter = (EPFDbWriter) Class.forName(
					EPF_DB_WRITER_MAP.get(dbConfig.getDbDriverClass()))
					.newInstance();
		} catch (InstantiationException e) {
			throw new EPFDbException(e.getMessage());
		} catch (IllegalAccessException e) {
			throw new EPFDbException(e.getMessage());
		} catch (ClassNotFoundException e) {
			throw new EPFDbException(e.getMessage());
		}
		
		dbWriter.setConnector(connector);

		return dbWriter;
	}
}
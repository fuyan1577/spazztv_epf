package com.spazzmania.epf.dao;

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
					put("com.mysql.jdbc.Driver",
							com.spazzmania.epf.mysql.EPFDbWriterMySql.class
									.getName());
				}
			});

	private static EPFDbWriterFactory factory = null;

	private EPFDbConnector connector;

	private EPFDbWriterFactory(EPFDbConfig dbConfig) throws EPFDbException {
		try {
			connector = new EPFDbConnector(dbConfig);
		} catch (EPFDbException e) {
			throw new EPFDbException(e.getMessage());
		}
	}

	public static EPFDbWriterFactory getInstance() {
		return factory;
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

	/**
	 * Return the EPFDbConnector object used by the factory instance.
	 * <p/>
	 * This is primarily used to close down the connection factory on
	 * completion.
	 * 
	 * @return connector
	 */
	public EPFDbConnector getConnector() {
		return connector;
	}

}
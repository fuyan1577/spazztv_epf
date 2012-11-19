package com.spazztv.epf.dao;

/**
 * @author Thomas Billingsley
 * @version 1.0
 */
public class EPFDbWriterFactory {

	private static EPFDbWriterFactory factory = null;

	private EPFDbConnector connector;

	private EPFDbWriterFactory(EPFDbConfig dbConfig) throws EPFDbException {
		connector = EPFDbConnector.getInstance(dbConfig);
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
		EPFDbWriter dbWriter = null;

		try {
			dbWriter = (EPFDbWriter) Class.forName(dbConfig.getDbWriterClass())
					.newInstance();
		} catch (Exception e) {
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

	/**
	 * Close the connectionPool. The method called closes the pool only if it is
	 * currently open.
	 * 
	 * @throws EPFDbException
	 */
	public static void closeFactory() throws EPFDbException {
		factory.getConnector().closeConnectionPool();
	}

}
package com.spazztv.epf.dao;

import java.sql.Connection;
import java.sql.SQLException;

import oracle.ucp.UniversalConnectionPoolAdapter;
import oracle.ucp.UniversalConnectionPoolException;
import oracle.ucp.admin.UniversalConnectionPoolManager;
import oracle.ucp.admin.UniversalConnectionPoolManagerImpl;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

/**
 * This is an object for wrapping a JDBC or NoSQL DB Connection Pool. The
 * current implementation uses Oracle Universal Connection Pool (UCP)
 * <p/>
 * The provided EPFDbConfig object is used to set up the Connection Pool object.
 * 
 * <p/>
 * Classes call the <i>getConnection()</i> method to retrieve a connection. The
 * connections are released back into the pool by calling the
 * <i>connection.close()</i> method.
 * <p/>
 * A call should be made to <i>closeConnectionPool()</i> at the end of database
 * processing.
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFDbConnector {
	public static Integer DEFAULT_MIN_CONNECTIONS = 5;
	public static Integer DEFAULT_MAX_CONNECTIONS = 20;
	public static String EPF_DB_POOL_NAME = "epf_db_pool";

	private UniversalConnectionPoolManager mgr;
	private PoolDataSource pds;
	private EPFDbConfig dbConfig;
	
	private static EPFDbConnector instance;

	private EPFDbConnector(EPFDbConfig dbConfig) {
		this.dbConfig = dbConfig;
	}
	
	public static EPFDbConnector getInstance(EPFDbConfig dbConfig) {
		if (instance == null) {
			instance = new EPFDbConnector(dbConfig);
		}
		return instance;
	}

	public final EPFDbConfig getEPFDbConfig() {
		return dbConfig;
	}

	public void openConnectionPool() throws EPFDbException {
		EPFDbConfig dbConfig = getEPFDbConfig();

		try {
			mgr = UniversalConnectionPoolManagerImpl
					.getUniversalConnectionPoolManager();

			pds = PoolDataSourceFactory.getPoolDataSource();
			pds.setConnectionPoolName(EPF_DB_POOL_NAME);

			pds.setConnectionFactoryClassName(dbConfig.getDbDataSourceClass());
			
			pds.setURL(dbConfig.getDbUrl());
			pds.setUser(dbConfig.getUsername());
			pds.setPassword(dbConfig.getPassword());

			mgr.createConnectionPool((UniversalConnectionPoolAdapter) pds);

			pds.setInitialPoolSize(dbConfig.getMinConnections());
			pds.setMinPoolSize(dbConfig.getMinConnections());
			pds.setMaxPoolSize(dbConfig.getMaxConnections());

		} catch (UniversalConnectionPoolException e) {
			throw new EPFDbException(e.getMessage());
		} catch (SQLException e) {
			throw new EPFDbException(e.getMessage());
		}
	}

	public void closeConnectionPool() throws EPFDbException {
		try {
			if (mgr != null) {
				mgr.purgeConnectionPool(EPF_DB_POOL_NAME);
			}
		} catch (UniversalConnectionPoolException e) {
			throw new EPFDbException(e.getMessage());
		}
	}

	public synchronized Connection getConnection() throws EPFDbException {
		if (pds == null) {
			openConnectionPool();
		}
		try {
			return pds.getConnection();
		} catch (SQLException e) {
			throw new EPFDbException(e.getMessage());
		}
	}

	public synchronized void releaseConnection(Object connection) throws EPFDbException {
		try {
			((Connection) connection).close();
			connection = null;
		} catch (SQLException e) {
			throw new EPFDbException(e.getMessage());
		}
	}
}

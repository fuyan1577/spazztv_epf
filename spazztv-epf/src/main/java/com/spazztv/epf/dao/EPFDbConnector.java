package com.spazztv.epf.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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

	public static Map<String, String> DB_FACTORY_CLASS_MAP = Collections
			.unmodifiableMap(new HashMap<String, String>() {
				private static final long serialVersionUID = 1L;
				{
					put("com.mysql.jdbc.Driver",
							com.mysql.jdbc.jdbc2.optional.MysqlDataSource.class
									.getName());
				}
			});

	public EPFDbConnector(EPFDbConfig dbConfig) throws EPFDbException {
		this.dbConfig = dbConfig;
		openConnectionPool();
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

			String factoryClassName = DB_FACTORY_CLASS_MAP.get(
					dbConfig.getDbDataSource());
			if (factoryClassName == null) {
				throw new EPFDbException(String.format(
						"Invalid/Unsupported Driver Class: %s",
						dbConfig.getDbDataSource()));
			}

			pds.setConnectionFactoryClassName(factoryClassName);

			pds.setURL(dbConfig.getDbUrl());
			pds.setUser(dbConfig.getUsername());
			pds.setPassword(dbConfig.getPassword());

			mgr.createConnectionPool((UniversalConnectionPoolAdapter) pds);

			pds.setInitialPoolSize(dbConfig.getMinConnections());
			pds.setMinPoolSize(dbConfig.getMinConnections());
			pds.setMaxPoolSize(dbConfig.getMaxConnections());

		} catch (SQLException e) {
			throw new EPFDbException(e.getMessage());
		} catch (UniversalConnectionPoolException e) {
			throw new EPFDbException(e.getMessage());
			// } catch (ClassNotFoundException e) {
			// throw new EPFDbException(e.getMessage());
		}

	}

	public void closeConnectionPool() throws EPFDbException {
		try {
			mgr.purgeConnectionPool(EPF_DB_POOL_NAME);
		} catch (UniversalConnectionPoolException e) {
			throw new EPFDbException(e.getMessage());
		}
	}

	public Connection getConnection() throws SQLException {
		return pds.getConnection();
	}

	public void releaseConnection(Object connection) throws EPFDbException {
		try {
			((Connection) connection).close();
			connection = null;
		} catch (SQLException e) {
			throw new EPFDbException(e.getMessage());
		}
	}
}

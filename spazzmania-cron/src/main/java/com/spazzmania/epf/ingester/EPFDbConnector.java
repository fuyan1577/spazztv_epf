package com.spazzmania.epf.ingester;

import java.sql.Connection;
import java.sql.SQLException;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

/**
 * This is an object for wrapping a JDBC or NoSQL DB Connection Pool. The
 * current implementation uses BoneCP.
 * 
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
	private BoneCP connectionPool;

	public static Long DEFAULT_MIN_CONNECTIONS = 5L;
	public static Long DEFAULT_MAX_CONNECTIONS = 20L;

	private EPFDbConfig dbConfig;

	public EPFDbConnector(EPFDbConfig dbConfig) {
		this.dbConfig = dbConfig;
	}

	public final EPFDbConfig getEPFDbConfig() {
		return dbConfig;
	}

	public void openConnectionPool() throws EPFDbException {
		EPFDbConfig dbConfig = getEPFDbConfig();
		BoneCPConfig config = new BoneCPConfig();
		try {
			Class.forName(dbConfig.getDbDriverClass());
			config.setJdbcUrl(dbConfig.getJdbcUrl());
			config.setDefaultCatalog(dbConfig.getDefaultCatalog());
			config.setUsername(dbConfig.getUsername());
			config.setPassword(dbConfig.getPassword());
			connectionPool = new BoneCP(config);
		} catch (Exception e) {
			throw new EPFDbException(e.getMessage());
		}
	}

	public void closeConnectionPool() throws EPFDbException {
		connectionPool.close();
	}

	public Connection getConnection() throws SQLException {
		return connectionPool.getConnection();
	}

	public void releaseConnection(Object connection) throws EPFDbException {
		try {
			((Connection) connection).close();
		} catch (SQLException e) {
			throw new EPFDbException(e.getMessage());
		}
	}
}

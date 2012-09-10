package com.spazzmania.epf.ingester;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

/**
 * This is an object for wrapping a JDBC Connection Pool. The current
 * implementation uses BoneCP.
 * 
 * <p/>
 * The provided EPFDbConfig object is used to set up the Connection Pool object.
 * 
 * <p/>
 * Classes call the <i>getConnection()</i> method to retrieve a connection. The
 * connections are released back into the pool by calling the <i>connection.close()</i>
 * method.
 * <p/>
 * A call should be made to <i>closeConnectionPool()</i> at the end of database processing.
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFDbConnector {

	public static Long DEFAULT_MIN_CONNECTIONS = 5L;
	public static Long DEFAULT_MAX_CONNECTIONS = 20L;

	private BoneCP connectionPool;
	private EPFDbConfig dbConfig;

	public EPFDbConnector(EPFDbConfig dbConfig) {
		this.dbConfig = dbConfig;
	}

	public EPFDbConfig getEPFDbConfig() {
		return dbConfig;
	}

	public void openConnectionPool(String configPath) throws IOException,
			ClassNotFoundException, SQLException {
		EPFDbConfig dbConfig = this.getEPFDbConfig();
		BoneCPConfig config = new BoneCPConfig();
		Class.forName(dbConfig.getJdbcDriverClass());
		config.setJdbcUrl(dbConfig.getJdbcUrl());
		config.setDefaultCatalog(dbConfig.getDefaultCatalog());
		config.setUsername(dbConfig.getUsername());
		config.setPassword(dbConfig.getPassword());
		connectionPool = new BoneCP(config);
	}

	public void closeConnectionPool() {
		connectionPool.close();
	}

	public Connection getConnection() throws SQLException {
		return connectionPool.getConnection();
	}
}

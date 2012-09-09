package com.spazzmania.epf.ingester;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

/**
 * This is the BoneCP Implementation of the EPFDbConnector abstract class. Per
 * the parent class, the logic is broken into the two following items:
 * <ol>
 * <li/>Load the JSON Configuration for the database connection pool - <i>implemented by parent class</i>
 * <li/>Return Connection object from the database connection pool - <i>implemented here</i>
 * </ol>
 * 
 * <p/>
 * The second item is done here using BoneCP Connection Pool Manager.
 * 
 * @author Thomas Billingsley
 */
public class EPFDbConnectorBoneCP extends EPFDbConnector {

	private BoneCP connectionPool;

	public EPFDbConnectorBoneCP(String configPath) throws IOException {
		super(configPath);
	}

	public void openConnectionPool(String configPath) throws IOException,
			ClassNotFoundException, SQLException {
		DBConfig dbConfig = this.getDBConfig();
		BoneCPConfig config = new BoneCPConfig();
		Class.forName(dbConfig.jdbcDriverClass);
		config.setJdbcUrl(dbConfig.jdbcUrl);
		config.setDefaultCatalog(dbConfig.defaultCatalog);
		config.setUsername(dbConfig.username);
		config.setPassword(dbConfig.password);
		connectionPool = new BoneCP(config);
	}

	public void closeConnectionPool() {
		connectionPool.close();
	}

	public Connection getConnection() throws SQLException {
		return connectionPool.getConnection();
	}
}

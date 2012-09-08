package com.spazzmania.epf.ingester;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

/**
 * This is a singleton object which provides two functionalities:
 * <ol>
 * <li/>Load the JSON Configuration for the database connection pool
 * <li/>Return Connection object from the database connection pool
 * </ol>
 * 
 * <p/>
 * The configuration is loaded from an EPFDBConnector.json configuration file
 * located in the local config path. The configuration file designates the JDBC
 * Class, schema, username and password to be used by the database connection
 * pool.
 * 
 * <p/>
 * The second function of this object is to work as a connection factory
 * accessed by the <i>getConnection()</i> method.
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFDbConnectorBoneCP extends EPFDbConnector {

	private BoneCP connectionPool;

	public EPFDbConnectorBoneCP(String configPath) throws IOException {
		super(configPath);
	}

	public void openConnectionPool(String configPath) throws IOException, ClassNotFoundException, SQLException {
		BoneCPConfig config = new BoneCPConfig();
		Class.forName(DBConfig.jdbcDriverClass);
		config.setJdbcUrl(DBConfig.jdbcUrl);
		config.setDefaultCatalog(DBConfig.defaultCatalog);
		config.setUsername(DBConfig.username);
		config.setPassword(DBConfig.password);
		
		connectionPool = new BoneCP(config);
	}
	
	public void closeConnectionPool() {
		connectionPool.close();
	}
	
	public Connection getConnection() throws SQLException {
		return connectionPool.getConnection();
	}
}

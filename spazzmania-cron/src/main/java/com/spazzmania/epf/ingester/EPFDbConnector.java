package com.spazzmania.epf.ingester;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

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
public class EPFDbConnector {

	private static EPFDbConnector connector;
	private BoneCP connectionPool;

	public static String CONNECTION_POOL = "connection-pool";
	public static String JDBC_DRIVER_CLASS = "jdbc-driver-class";
	public static String JDBC_DEFAULT_CATALOG = "default-catalog";
	public static String JDBC_MIN_CONNECTIONS = "min-connections";
	public static String JDBC_MAX_CONNECTIONS = "max-connections";
	public static String JDBC_URL = "jdbc-url";
	public static String JDBC_USERNAME = "username";
	public static String JDBC_PASSWORD = "password";
	public static Integer DEFAULT_MIN_CONNECTIONS = 5;
	public static Integer DEFAULT_MAX_CONNECTIONS = 20;

	private static class DBConfig {
		static String jdbcDriverClass;
		static String defaultCatalog;
		static String jdbcUrl;
		static String username;
		static String password;
		static int minConnections;
		static int maxConnections;
	}

	private EPFDbConnector() {
	}

	public static EPFDbConnector getInstance() {
		if (connector == null) {
			connector = new EPFDbConnector();
		}

		return connector;
	}

	public void openConnectionPool(String configPath) throws IOException, ClassNotFoundException, SQLException {
		String configJson = loadConfigFile(configPath);
		parseConfiguration(configJson);
		
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
	
	public void parseConfiguration(String configJson) {
		JSONParser parser = new JSONParser();
		JSONObject connPoolObject;
		try {
			connPoolObject = (JSONObject) parser.parse(configJson);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
		
		DBConfig.jdbcDriverClass = (String)connPoolObject.get(JDBC_DRIVER_CLASS);
		DBConfig.defaultCatalog = (String)connPoolObject.get(JDBC_DEFAULT_CATALOG);
		DBConfig.jdbcUrl = (String)connPoolObject.get(JDBC_URL);
		DBConfig.username = (String)connPoolObject.get(JDBC_USERNAME);
		DBConfig.password = (String)connPoolObject.get(JDBC_PASSWORD);
		DBConfig.minConnections = (Integer)connPoolObject.get(JDBC_MIN_CONNECTIONS);
		DBConfig.maxConnections = (Integer)connPoolObject.get(JDBC_MAX_CONNECTIONS);
		if (DBConfig.minConnections <= 0) {
			DBConfig.minConnections = DEFAULT_MIN_CONNECTIONS;
		}
		if (DBConfig.maxConnections <= 0) {
			DBConfig.maxConnections = DEFAULT_MAX_CONNECTIONS;
		}
	}

	private String loadConfigFile(String configFilePath) throws IOException {
		FileInputStream stream = new FileInputStream(new File(configFilePath));
		try {
			FileChannel fc = stream.getChannel();
			MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0,
					fc.size());
			/* Instead of using default, pass in a decoder. */
			return Charset.defaultCharset().decode(bb).toString();
		} finally {
			stream.close();
		}
	}

	public Connection getConnection() throws SQLException {
		return connectionPool.getConnection();
	}
}

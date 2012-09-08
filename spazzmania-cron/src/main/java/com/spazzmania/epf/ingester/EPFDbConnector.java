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

/**
 * This is an abstract object for wrapping a JDBC Connection Pool. The defined
 * methods load the JSON formatted configuration.
 * 
 * <p/>
 * The logic is broken into the two following steps:
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
 * The second function of this object is to open the database connection pool
 * and return connections through the <i>getConnection()</i> method.
 * 
 * <p/>
 * Implementing classes which extend this class load the actual database
 * connection pool object.
 * 
 * @author Thomas Billingsley
 * 
 */
public abstract class EPFDbConnector {

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

	public static class DBConfig {
		static String jdbcDriverClass;
		static String defaultCatalog;
		static String jdbcUrl;
		static String username;
		static String password;
		static int minConnections;
		static int maxConnections;
	}

	public EPFDbConnector(String configPath) throws IOException {
		String configJson = loadConfigFile(configPath);
		parseConfiguration(configJson);
	}

	public abstract void openConnectionPool(String configPath)
			throws IOException, ClassNotFoundException, SQLException;

	public abstract void closeConnectionPool();

	public abstract Connection getConnection() throws SQLException;

	public void parseConfiguration(String configJson) {
		JSONParser parser = new JSONParser();
		JSONObject connPoolObject;
		try {
			connPoolObject = (JSONObject) parser.parse(configJson);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}

		DBConfig.jdbcDriverClass = (String) connPoolObject
				.get(JDBC_DRIVER_CLASS);
		DBConfig.defaultCatalog = (String) connPoolObject
				.get(JDBC_DEFAULT_CATALOG);
		DBConfig.jdbcUrl = (String) connPoolObject.get(JDBC_URL);
		DBConfig.username = (String) connPoolObject.get(JDBC_USERNAME);
		DBConfig.password = (String) connPoolObject.get(JDBC_PASSWORD);
		DBConfig.minConnections = (Integer) connPoolObject
				.get(JDBC_MIN_CONNECTIONS);
		DBConfig.maxConnections = (Integer) connPoolObject
				.get(JDBC_MAX_CONNECTIONS);
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

}

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
 * <li/>Load the JSON Configuration for the database connection pool -
 * <i>Implemented in this class</i>
 * <li/>Return Connection object from the database connection pool -
 * <i>Implemented by child classes</i>
 * </ol>
 * 
 * <p/>
 * The configuration is loaded from an EPFDbConnector.json configuration file
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
	public static Long DEFAULT_MIN_CONNECTIONS = 5L;
	public static Long DEFAULT_MAX_CONNECTIONS = 20L;

	public class DBConfig {
		public String jdbcDriverClass;
		public String defaultCatalog;
		public String jdbcUrl;
		public String username;
		public String password;
		public Long minConnections;
		public Long maxConnections;
	}

	private DBConfig dbConfig;

	public EPFDbConnector() {
		dbConfig = new DBConfig();
	}

	public EPFDbConnector(String configFilePath) throws IOException {
		dbConfig = new DBConfig();
		String configJson = loadConfigFile(configFilePath);
		parseConfiguration(configJson);
	}

	public abstract void openConnectionPool(String configPath)
			throws IOException, ClassNotFoundException, SQLException;

	public abstract void closeConnectionPool();

	public abstract Connection getConnection() throws SQLException;

	public DBConfig getDBConfig() {
		return dbConfig;
	}

	public void parseConfiguration(String configJson) {
		JSONParser parser = new JSONParser();
		JSONObject connPoolObject;
		try {
			JSONObject json = (JSONObject) parser.parse(configJson);
			connPoolObject = (JSONObject) json.get(CONNECTION_POOL);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}

		// Required Values
		dbConfig.jdbcDriverClass = verifyString(connPoolObject,
				JDBC_DRIVER_CLASS);
		dbConfig.jdbcUrl = verifyString(connPoolObject, JDBC_URL);
		dbConfig.username = verifyString(connPoolObject, JDBC_USERNAME);
		dbConfig.password = verifyString(connPoolObject, JDBC_PASSWORD);

		// Optional values
		dbConfig.defaultCatalog = (String) connPoolObject
				.get(JDBC_DEFAULT_CATALOG);
		dbConfig.minConnections = verifyLongDefault(connPoolObject,
				JDBC_MIN_CONNECTIONS, DEFAULT_MIN_CONNECTIONS);
		dbConfig.maxConnections = verifyLongDefault(connPoolObject,
				JDBC_MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS);
	}

	private String verifyString(JSONObject connPoolObject, String key) {
		if (connPoolObject.get(key) == null) {
			throw new RuntimeException(String.format(
					"Missing EPFDbConnector Parameter: %s", key));
		}
		return (String) connPoolObject.get(key);
	}

	private Long verifyLongDefault(JSONObject connPoolObject, String key,
			Long defaultValue) {
		if (connPoolObject.get(key) == null) {
			return defaultValue;
		}
		return (Long) connPoolObject.get(key);
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

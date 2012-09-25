package com.spazzmania.epf.dao;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


/**
 * This object is a simple pojo for holding datasbase configuration values for
 * use by the Database Connection Pool.
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFDbConfig {
	public static String CONNECTION_POOL = "dbConnectionPool";
	public static String DB_DRIVER_CLASS = "dbDriverClass";
	public static String DB_DEFAULT_CATALOG = "dbDefaultCatalog";
	public static String DB_MIN_CONNECTIONS = "dbMinConnections";
	public static String DB_MAX_CONNECTIONS = "dbMaxConnections";
	public static String DB_URL = "dbJdbcUrl";
	public static String DB_USER = "dbUser";
	public static String DB_PASSWORD = "dbPassword";
	
	private String dbDriverClass;
	private String defaultCatalog;
	private String jdbcUrl;
	private String username;
	private String password;
	private Long minConnections;
	private Long maxConnections;
	private String tablePrefix;
	
	public EPFDbConfig() {
	}
	
	public EPFDbConfig(String configFilePath) throws IOException {
		String configJson = loadConfigFile(configFilePath);
		parseConfiguration(configJson);
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
		dbDriverClass = verifyString(connPoolObject,
				DB_DRIVER_CLASS);
		jdbcUrl = verifyString(connPoolObject, DB_URL);
		username = verifyString(connPoolObject, DB_USER);
		password = verifyString(connPoolObject, DB_PASSWORD);

		// Optional values
		defaultCatalog = (String) connPoolObject
				.get(DB_DEFAULT_CATALOG);
		minConnections = verifyLongDefault(connPoolObject,
				DB_MIN_CONNECTIONS, EPFDbConnector.DEFAULT_MIN_CONNECTIONS);
		maxConnections = verifyLongDefault(connPoolObject,
				DB_MAX_CONNECTIONS, EPFDbConnector.DEFAULT_MAX_CONNECTIONS);
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
	
	
	public String getDbDriverClass() {
		return dbDriverClass;
	}
	public void setJdbcDriverClass(String dbDriverClass) {
		this.dbDriverClass = dbDriverClass;
	}
	public String getDefaultCatalog() {
		return defaultCatalog;
	}
	public void setDefaultCatalog(String defaultCatalog) {
		this.defaultCatalog = defaultCatalog;
	}
	public String getJdbcUrl() {
		return jdbcUrl;
	}
	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public Long getMinConnections() {
		return minConnections;
	}
	public void setMinConnections(Long minConnections) {
		this.minConnections = minConnections;
	}
	public Long getMaxConnections() {
		return maxConnections;
	}
	public void setMaxConnections(Long maxConnections) {
		this.maxConnections = maxConnections;
	}
	public String getTablePrefix() {
		return tablePrefix;
	}
	public void setTablePrefix(String tablePrefix) {
		this.tablePrefix = tablePrefix;
	}
}

package com.spazzmania.epf.ingester;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class EPFDbConnectorTest {

	private EPFDbConnector connector;
	private String epfDbConfigJson;
	private String epfDbConfigJson2;

	class EPFDbConnectorMock extends EPFDbConnector {

		public EPFDbConnectorMock() {
		}

		public EPFDbConnectorMock(String configPath) throws IOException {
			super(configPath);
		}

		@Override
		public void openConnectionPool(String configPath) throws IOException,
				ClassNotFoundException, SQLException {
		}

		@Override
		public void closeConnectionPool() {
		}

		@Override
		public Connection getConnection() throws SQLException {
			return null;
		}
	}

	@Before
	public void setUp() {
		connector = new EPFDbConnectorMock();
		epfDbConfigJson = "{	\"connection-pool\" : {"
				+ "	\"jdbc-driver-class\": \"com.lang.Object\","
				+ "	\"default-catalog\": \"mockcatalog\","
				+ "	\"min-connections\": 6," + "	\"max-connections\": 21,"
				+ "	\"jdbc-url\": \"jdbc:mockdb://localhost/mockdb\","
				+ "	\"username\": \"mockusername\","
				+ "	\"password\": \"mockpassword\"	" + "	}" + "}";
		epfDbConfigJson2 = "{	\"connection-pool\" : {"
				+ "	\"jdbc-driver-class\": \"com.lang.Object\","
				+ "	\"default-catalog\": \"mockcatalog\","
				+ "	\"jdbc-url\": \"jdbc:mockdb://localhost/mockdb\","
				+ "	\"username\": \"mockusername\","
				+ "	\"password\": \"mockpassword\"	" + "	}" + "}";
	}

	// @Test
	// public void testEPFDbConnector() {
	// fail("Not yet implemented");
	// }

	@Test
	public void testParseConfiguration() {
		connector.parseConfiguration(epfDbConfigJson);
		EPFDbConnector.DBConfig config = connector.getDBConfig();
		Assert.assertTrue("Invalid jdbcDriverClass", config.jdbcDriverClass.equals("com.lang.Object"));
		Assert.assertTrue("Invalid jdbcUrl", config.jdbcUrl.equals("jdbc:mockdb://localhost/mockdb"));
		Assert.assertTrue("Invalid defaultCatalog", config.defaultCatalog.equals("mockcatalog"));
		Assert.assertTrue("Invalid config username", config.username.equals("mockusername"));
		Assert.assertTrue("Invalid config password", config.password.equals("mockpassword"));
		Assert.assertTrue("Invalid minConnections", config.minConnections == 6);
		Assert.assertTrue("Invalid maxConnections", config.maxConnections == 21);
	}
	
	@Test
	public void testParseConfigurationWithDefaults() {
		connector.parseConfiguration(epfDbConfigJson2);
		EPFDbConnector.DBConfig config = connector.getDBConfig();
		//Test for the default min & max connection values
		Assert.assertTrue("Invalid minConnections", config.minConnections == 5);
		Assert.assertTrue("Invalid maxConnections", config.maxConnections == 20);
	}
}

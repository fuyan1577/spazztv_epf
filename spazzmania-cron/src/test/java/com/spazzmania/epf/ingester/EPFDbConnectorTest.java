package com.spazzmania.epf.ingester;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.spazzmania.epf.ingester.EPFDbConnector.DBConfig;

public class EPFDbConnectorTest {

	private EPFDbConnector connector;
	private String epfDbConfigJson;
	private String epfDbConfigJson2;
	private String epfDbConfigPath;

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
		epfDbConfigJson = "{" + "	\"dbConnectionPool\": {"
				+ "		\"dbJdbcDriverClass\":\"com.mysql.jdbc.Driver\","
				+ "		\"dbDefaultCatalog\":\"mockcatalog\","
				+ "		\"dbMinConnections\":6," + "		\"dbMaxConnections\":21,"
				+ "		\"dbJdbcUrl\": \"jdbc:mysql://localhost/mockdb\", "
				+ "		\"dbUser\": \"mockusername\","
				+ "		\"dbPassword\": \"mockpassword\"" + "	}" + "}";
		epfDbConfigJson2 = "{" + "	\"dbConnectionPool\": {"
				+ "		\"dbJdbcDriverClass\":\"com.mysql.jdbc.Driver\","
				+ "		\"dbDefaultCatalog\":\"mockcatalog\","
				+ "		\"dbJdbcUrl\": \"jdbc:mysql://localhost/mockdb\", "
				+ "		\"dbUser\": \"mockusername\","
				+ "		\"dbPassword\": \"mockpassword\"" + "	}" + "}";

		URL epfDbConfigURL = EPFDbConnectorTest.class
				.getResource("EPFDbConfig.json");
		if (epfDbConfigURL != null) {
			try {
				epfDbConfigPath = epfDbConfigURL.toURI().toString();
				epfDbConfigPath = epfDbConfigPath.replaceFirst("file:/", "");
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		}
	}

	@Test
	public void testParseConfiguration() {
		connector.parseConfiguration(epfDbConfigJson);
		EPFDbConnector.DBConfig config = connector.getDBConfig();
		Assert.assertTrue("Invalid jdbcDriverClass",
				config.jdbcDriverClass.equals("com.mysql.jdbc.Driver"));
		Assert.assertTrue("Invalid jdbcUrl",
				config.jdbcUrl.equals("jdbc:mysql://localhost/mockdb"));
		Assert.assertTrue("Invalid defaultCatalog",
				config.defaultCatalog.equals("mockcatalog"));
		Assert.assertTrue("Invalid config username",
				config.username.equals("mockusername"));
		Assert.assertTrue("Invalid config password",
				config.password.equals("mockpassword"));
		Assert.assertTrue("Invalid minConnections", config.minConnections == 6);
		Assert.assertTrue("Invalid maxConnections", config.maxConnections == 21);
	}

	@Test
	public void testParseConfigurationWithDefaults() {
		connector.parseConfiguration(epfDbConfigJson2);
		EPFDbConnector.DBConfig config = connector.getDBConfig();
		// Test for the default min & max connection values
		Assert.assertTrue("Invalid minConnections", config.minConnections == 5);
		Assert.assertTrue("Invalid maxConnections", config.maxConnections == 20);
	}

	@Test
	public void testEPFDbConnector() throws IOException {
		Assert.assertNotNull("EPFDbConfig.json not found for JUnit test",
				epfDbConfigPath);
		connector = new EPFDbConnectorMock(epfDbConfigPath);
		DBConfig config = connector.getDBConfig();
		Assert.assertNotNull("Invalid dbJdbcDriverClass",config.jdbcDriverClass);
		Assert.assertNotNull("Invalid dbJdbcUrl",config.jdbcUrl);
		Assert.assertNotNull("Invalid dbDefaultCatalog",config.defaultCatalog);
		Assert.assertNotNull("Invalid dbUser",config.username);
		Assert.assertNotNull("Invalid dbPassword",config.password);
		Assert.assertNotNull("Invalid dbMinConnections",config.minConnections);
		Assert.assertNotNull("Invalid dbMaxConnections",config.maxConnections);
	}
}

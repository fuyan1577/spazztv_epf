package com.spazztv.epf.dao;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.spazztv.epf.EPFImporterException;

public class EPFDbConfigTest {

	private EPFDbConfig dbConfig;
	private String epfDbConfigJson;
	private String epfDbConfigJson2;
	private String epfDbConfigPath;

	@Before
	public void setUp() {
		dbConfig = new EPFDbConfig();
		epfDbConfigJson = "{" + "	\"dbConnection\": {"
				+ "		\"dbWriter\":\"com.spazztv.epf.adapter.EPFDbWriterMySql\","
				+ "		\"dbDataSource\":\"com.mysql.jdbc.Driver\","
				+ "		\"dbDefaultCatalog\":\"mockcatalog\","
				+ "		\"dbMinConnections\":6," + "		\"dbMaxConnections\":21,"
				+ "		\"dbUrl\": \"jdbc:mysql://localhost/mockdb\", "
				+ "		\"dbUser\": \"mockusername\","
				+ "		\"dbPassword\": \"mockpassword\"" + "	}" + "}";
		epfDbConfigJson2 = "{" + "	\"dbConnection\": {"
				+ "		\"dbWriter\":\"com.spazztv.epf.adapter.EPFDbWriterMySql\","
				+ "		\"dbDataSource\":\"com.mysql.jdbc.Driver\","
				+ "		\"dbDefaultCatalog\":\"mockcatalog\","
				+ "		\"dbUrl\": \"jdbc:mysql://localhost/mockdb\", "
				+ "		\"dbUser\": \"mockusername\","
				+ "		\"dbPassword\": \"mockpassword\"" + "	}" + "}";

		URL epfDbConfigURL = EPFDbConfigTest.class
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
	public void testParseConfiguration() throws EPFImporterException {
		dbConfig.parseConfiguration(epfDbConfigJson);
		Assert.assertTrue("Invalid dbWriter",
				dbConfig.getDbWriter().equals("com.spazztv.epf.adapter.EPFDbWriterMySql"));
		Assert.assertTrue("Invalid dbDataSource",
				dbConfig.getDbDataSource().equals("com.mysql.jdbc.Driver"));
		Assert.assertTrue("Invalid jdbcUrl",
				dbConfig.getDbUrl().equals("jdbc:mysql://localhost/mockdb"));
		Assert.assertTrue("Invalid defaultCatalog",
				dbConfig.getDefaultCatalog().equals("mockcatalog"));
		Assert.assertTrue("Invalid config username",
				dbConfig.getUsername().equals("mockusername"));
		Assert.assertTrue("Invalid config password",
				dbConfig.getPassword().equals("mockpassword"));
		Assert.assertTrue("Invalid minConnections", dbConfig.getMinConnections() == 6);
		Assert.assertTrue("Invalid maxConnections", dbConfig.getMaxConnections() == 21);
	}

	@Test
	public void testParseConfigurationWithDefaults() throws EPFImporterException {
		dbConfig.parseConfiguration(epfDbConfigJson2);
		// Test for the default min & max connection values
		Assert.assertTrue("Invalid minConnections", dbConfig.getMinConnections() == 5);
		Assert.assertTrue("Invalid maxConnections", dbConfig.getMaxConnections() == 20);
	}

	@Test
	public void testEPFDbConnector() throws IOException, EPFImporterException {
		Assert.assertNotNull("EPFDbConfig.json not found for JUnit test",
				epfDbConfigPath);
		EPFDbConfig config = new EPFDbConfig(new File(epfDbConfigPath));
		Assert.assertNotNull("Invalid dbWriter",config.getDbDataSource());
		Assert.assertNotNull("Invalid dbDataSource",config.getDbDataSource());
		Assert.assertNotNull("Invalid dbJdbcUrl",config.getDbUrl());
		Assert.assertNotNull("Invalid dbDefaultCatalog",config.getDefaultCatalog());
		Assert.assertNotNull("Invalid dbUser",config.getUsername());
		Assert.assertNotNull("Invalid dbPassword",config.getPassword());
		Assert.assertNotNull("Invalid dbMinConnections",config.getMinConnections());
		Assert.assertNotNull("Invalid dbMaxConnections",config.getMaxConnections());
	}
}

package com.spazzmania.epf.dao;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class EPFDbConnectorTest {

	private EPFDbConnector connector;
	private EPFDbConfig dbConfig;

	@Before
	public void setUp() throws Exception {
		dbConfig = new EPFDbConfig();
		dbConfig.setJdbcDriverClass("com.jdbc.mysql.Driver");
		dbConfig.setJdbcUrl("jdbc:mysql://localhost:3306/epf");
		dbConfig.setUsername("webaccess");
		dbConfig.setPassword("wsc2aofi");
		// --dburl jdbc:mysql://localhost:3306/epf -u webaccess -p wsc2aofi
		// -l com.mysql.jdbc.Driver
	}

	@Test
	public void testOpenConnectionPool() throws EPFDbException {
		connector = new EPFDbConnector(dbConfig);
		Assert.assertTrue("Place holder string test", true);
	}

	@Test
	public void testCloseConnectionPool() {
		Assert.assertTrue("Place holder string test", true);
	}

	@Test
	public void testGetConnection() {
		Assert.assertTrue("Place holder string test", true);
	}

	@Test
	public void testReleaseConnection() {
		Assert.assertTrue("Place holder string test", true);
	}

}

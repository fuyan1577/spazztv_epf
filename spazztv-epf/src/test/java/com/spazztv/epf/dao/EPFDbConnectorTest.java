package com.spazztv.epf.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class EPFDbConnectorTest {

	private static EPFDbConnector connector;
	private static EPFDbConfig dbConfig;

	private Connection connection;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		dbConfig = new EPFDbConfig();
		dbConfig.setDbDataSourceClass("com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
		dbConfig.setDbUrl("jdbc:mysql://localhost:3306/epf");
		dbConfig.setUsername("webaccess");
		dbConfig.setPassword("wsc2aofi");
		if (connector == null) {
			connector = EPFDbConnector.getInstance(dbConfig);
		}
		// --dburl jdbc:mysql://localhost:3306/epf -u webaccess -p wsc2aofi
		// -l com.mysql.jdbc.Driver
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		connector.closeConnectionPool();
	}

	@Test
	public void testDbDataSource() {
		String factoryClassName = dbConfig.getDbDataSourceClass();

		Assert.assertTrue(String.format(
				"Unable to retrieve DB Factory Class for %s",
				dbConfig.getDbDataSourceClass()), factoryClassName != null);
	}

	@Test
	public void testOpenConnectionPool() throws EPFDbException {
		Assert.assertTrue("Error instantiaating Connection Pool",
				connector != null);
	}

	@Test
	public void testOpenAndCloseConnection() throws EPFDbException,
			SQLException {
		connection = connector.getConnection();
		Assert.assertTrue("Error retrieving connection from Pool",
				connection != null);
		Assert.assertTrue("Error retrieving connection from Pool",
				!connection.isClosed());
		connection.close();
		Assert.assertTrue("Error closing connection from Pool",
				connection.isClosed());
	}

	@Test
	public void testConnectionQuery() throws EPFDbException, SQLException {
		connection = connector.getConnection();
		Statement stmt = connection.createStatement();
		ResultSet result = stmt
				.executeQuery("select 'dummy_value' as dummy_column");
		Assert.assertTrue("Invalid result set from test query", result.next());
		Assert.assertTrue("Invalid result set from test query",
				result.getRow() == 1);
		connection.close();
	}
}

package com.spazztv.epf.adapter;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.spazztv.epf.dao.EPFDbConnector;
import com.spazztv.epf.dao.EPFDbException;

public class EPFDbWriterMySqlDaoTest {

	private EPFDbWriterMySqlDao mySqlDao;
	private EPFDbConnector connector;
	private Connection connection;
	private Statement statement;
	private PreparedStatement preparedStatement;
	private ResultSet resultSet;
	private ResultSetMetaData metaData;
	private DatabaseMetaData dbMetaData;

	@Before
	public void setUp() throws Exception {
		System.setProperty("log4j.defaultInitOverride","Override");
		
		connector = EasyMock.createMock(EPFDbConnector.class);
		connection = EasyMock.createMock(Connection.class);
		statement = EasyMock.createMock(Statement.class);
		preparedStatement = EasyMock.createMock(PreparedStatement.class);
		resultSet = EasyMock.createMock(ResultSet.class);
		dbMetaData = EasyMock.createMock(DatabaseMetaData.class);
		metaData = EasyMock.createMock(ResultSetMetaData.class);
		EPFDbWriterMySql dbWriter = new EPFDbWriterMySql();
		dbWriter.setConnector(connector);

		mySqlDao = new EPFDbWriterMySqlDao(dbWriter);
	}

	@Test
	public void testExecuteSQLStatement() throws SQLException, EPFDbException {
		String expectedStatement = "SELECT * FROM test_table LIMIT 1";

		EasyMock.reset(connector);
		connector.getConnection();
		EasyMock.expectLastCall().andReturn(connection).times(1);
		connector.releaseConnection(connection);
		EasyMock.expectLastCall().times(1);
		EasyMock.replay(connector);

		EasyMock.reset(connection);
		connection.prepareStatement(expectedStatement);
		EasyMock.expectLastCall().andReturn(preparedStatement).times(1);
		EasyMock.replay(connection);

		EasyMock.reset(preparedStatement);
		preparedStatement.execute();
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(preparedStatement);

		mySqlDao.executeSQLStatement(expectedStatement, null);

		EasyMock.verify(connector);
		EasyMock.verify(connection);
		EasyMock.verify(preparedStatement);
	}

	@Test
	public void testExecuteSQLStatementError() throws SQLException,
			EPFDbException {
		String expectedStatement = "SELECT * FROM test_table LIMIT 1";

		EasyMock.reset(connector);
		connector.getConnection();
		EasyMock.expectLastCall().andReturn(connection).times(1);
		connector.releaseConnection(connection);
		EasyMock.expectLastCall().times(1);
		EasyMock.replay(connector);

		EasyMock.reset(connection);
		connection.prepareStatement(expectedStatement);
		EasyMock.expectLastCall().andReturn(preparedStatement).times(1);
		EasyMock.replay(connection);

		// Set statement to throw a SQLException to return an invalid status
		String expectedSQLState = "23000";
		int expectedErrorCode = 1205;
		SQLException expectedException = EasyMock
				.createMock(SQLException.class);
		expectedException.getSQLState();
		EasyMock.expectLastCall().andReturn(expectedSQLState).times(2);
		expectedException.getErrorCode();
		EasyMock.expectLastCall().andReturn(expectedErrorCode).times(2);
		EasyMock.replay(expectedException);

		EasyMock.reset(preparedStatement);
		preparedStatement.execute();
		EasyMock.expectLastCall().andThrow(expectedException);
		EasyMock.replay(preparedStatement);

		boolean expectedSuccess = false;
		SQLReturnStatus actual = mySqlDao
				.executeSQLStatement(expectedStatement, null);

		Assert.assertTrue(
				"Statement should have returned isSuccess() == false",
				expectedSuccess == false);
		Assert.assertTrue(
				String.format(
						"Unxpected getSQLState(), expected: %s, actual %s",
						expectedException.getSQLState(),
						actual.getSqlStateCode()),
				expectedSQLState.substring(0, 2).equals(
						actual.getSqlStateCode()));
		Assert.assertTrue(
				String.format(
						"Unxpected getErrorCode(), expected: %d, actual %d",
						expectedException.getErrorCode(),
						actual.getSqlExceptionCode()),
				expectedErrorCode == actual.getSqlExceptionCode());
		EasyMock.verify(connector);
		EasyMock.verify(connection);
		EasyMock.verify(preparedStatement);
	}

	@Test
	public void testGetTableColumns() throws SQLException, EPFDbException {
		String expectedStatement = "SELECT * FROM `test_table` LIMIT 1";
		String expectedTable = "test_table";

		EasyMock.reset(connector);
		connector.getConnection();
		EasyMock.expectLastCall().andReturn(connection).times(1);
		connector.releaseConnection(connection);
		EasyMock.expectLastCall().times(1);
		EasyMock.replay(connector);

		EasyMock.reset(connection);
		connection.createStatement();
		EasyMock.expectLastCall().andReturn(statement).times(1);
		EasyMock.replay(connection);

		// This test is done with a statement other than the
		// one declared here to generate an error.
		EasyMock.reset(statement);
		statement.executeQuery(EasyMock.eq(expectedStatement));
		EasyMock.expectLastCall().andReturn(resultSet).times(1);
		EasyMock.replay(statement);

		List<String> expectedColumns = new ArrayList<String>();
		expectedColumns.add("column1");
		expectedColumns.add("column2");
		expectedColumns.add("column3");

		EasyMock.reset(metaData);
		metaData.getColumnCount();
		EasyMock.expectLastCall().andReturn(3).times(4);
		metaData.getColumnName(0);
		EasyMock.expectLastCall().andReturn(expectedColumns.get(0)).times(1);
		metaData.getColumnName(1);
		EasyMock.expectLastCall().andReturn(expectedColumns.get(1)).times(1);
		metaData.getColumnName(2);
		EasyMock.expectLastCall().andReturn(expectedColumns.get(2)).times(1);
		EasyMock.replay(metaData);

		EasyMock.reset(resultSet);
		resultSet.getMetaData();
		EasyMock.expectLastCall().andReturn(metaData).times(1);
		EasyMock.replay(resultSet);

		List<String> actual = mySqlDao.getTableColumns(expectedTable);

		Assert.assertTrue(String.format(
				"Invalid number of columns, expected %d, actual %d",
				expectedColumns.size(), actual.size()),
				expectedColumns.size() == actual.size());

		for (int i = 0; i < expectedColumns.size(); i++) {
			Assert.assertTrue(String.format(
					"Unexpected column, expected %s, actual %s",
					expectedColumns.get(i), actual.get(i)), expectedColumns
					.get(i).equals(actual.get(i)));
		}

		EasyMock.verify(connector);
		EasyMock.verify(connection);
		EasyMock.verify(statement);
		EasyMock.verify(resultSet);
		EasyMock.verify(metaData);
	}

	@Test
	public void testIsTableInDatabase() throws SQLException, EPFDbException {
		String expectedTable = "test_table";
		String[] expectedTables = { "TABLE" };

		EasyMock.reset(connector);
		connector.getConnection();
		EasyMock.expectLastCall().andReturn(connection).times(1);
		connector.releaseConnection(connection);
		EasyMock.expectLastCall().times(1);
		EasyMock.replay(connector);

		EasyMock.reset(connection);
		connection.getMetaData();
		EasyMock.expectLastCall().andReturn(dbMetaData).times(1);
		EasyMock.replay(connection);

		EasyMock.reset(dbMetaData);
		dbMetaData.getTables((String) EasyMock.isNull(),
				(String) EasyMock.isNull(), EasyMock.eq(expectedTable),
				EasyMock.aryEq(expectedTables));
		EasyMock.expectLastCall().andReturn(resultSet).times(1);
		EasyMock.replay(dbMetaData);

		EasyMock.reset(resultSet);
		resultSet.beforeFirst();
		EasyMock.expectLastCall().times(1);
		resultSet.next();
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(resultSet);

		boolean expectedResult = true;
		boolean actual = mySqlDao.isTableInDatabase(expectedTable);

		Assert.assertTrue(
				String.format(
						"Invalid response from isTableInDatabase(), expected %s, actual %s",
						String.valueOf(expectedResult), String.valueOf(actual)),
				actual == expectedResult);
		
		EasyMock.verify(connector);
		EasyMock.verify(connection);
		EasyMock.verify(dbMetaData);
		EasyMock.verify(resultSet);
	}

	@Test
	public void testIsNotTableInDatabase() throws SQLException, EPFDbException {
		String expectedTable = "test_table";
		String[] expectedTables = { "TABLE" };

		EasyMock.reset(connector);
		connector.getConnection();
		EasyMock.expectLastCall().andReturn(connection).times(1);
		connector.releaseConnection(connection);
		EasyMock.expectLastCall().times(1);
		EasyMock.replay(connector);

		EasyMock.reset(connection);
		connection.getMetaData();
		EasyMock.expectLastCall().andReturn(dbMetaData).times(1);
		EasyMock.replay(connection);

		EasyMock.reset(dbMetaData);
		dbMetaData.getTables((String) EasyMock.isNull(),
				(String) EasyMock.isNull(), EasyMock.eq(expectedTable),
				EasyMock.aryEq(expectedTables));
		EasyMock.expectLastCall().andReturn(resultSet).times(1);
		EasyMock.replay(dbMetaData);

		EasyMock.reset(resultSet);
		resultSet.beforeFirst();
		EasyMock.expectLastCall().times(1);
		resultSet.next();
		EasyMock.expectLastCall().andReturn(false).times(1);
		EasyMock.replay(resultSet);

		boolean expectedResult = false;
		boolean actual = mySqlDao.isTableInDatabase(expectedTable);

		Assert.assertTrue(
				String.format(
						"Invalid response from isTableInDatabase(), expected %s, actual %s",
						String.valueOf(expectedResult), String.valueOf(actual)),
				actual == expectedResult);
		
		EasyMock.verify(connector);
		EasyMock.verify(connection);
		EasyMock.verify(dbMetaData);
		EasyMock.verify(resultSet);
	}
}

package com.spazzmania.epf.ingester;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;

import junit.framework.Assert;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class EPFDbWriterMySQLTest {

	EPFDbWriter dbWriter;
	EPFDbConnector connector;
	Connection connection;
	Statement statement;
	ResultSet resultSet;
	MockMetaData metaData;
	String tablePrefix = "test_";
	String tableName = "epftable";
	LinkedHashMap<String, String> columnsAndTypes;
	String[] primaryKey;

	@Before
	public void setUp() throws Exception {
		connector = EasyMock.createMock(EPFDbConnector.class);
		connection = EasyMock.createMock(Connection.class);
		statement = EasyMock.createMock(Statement.class);
		resultSet = EasyMock.createMock(ResultSet.class);
		metaData = new MockMetaData();
		dbWriter = new EPFDbWriterMySQL();
		dbWriter.setConnector(connector);
		dbWriter.setTablePrefix(tablePrefix);

		columnsAndTypes = new LinkedHashMap<String, String>();
		columnsAndTypes.put("export_date", "BIGINT");
		columnsAndTypes.put("application_id", "INTEGER");
		columnsAndTypes.put("title", "VARCHAR(1000)");
		columnsAndTypes.put("recommended_age", "VARCHAR(20)");
		columnsAndTypes.put("artist_name", "VARCHAR(1000)");
		columnsAndTypes.put("seller_name", "VARCHAR(1000)");
		columnsAndTypes.put("company_url", "VARCHAR(1000)");
		columnsAndTypes.put("support_url", "VARCHAR(1000)");
		columnsAndTypes.put("view_url", "VARCHAR(1000)");
		columnsAndTypes.put("artwork_url_large", "VARCHAR(1000)");
		columnsAndTypes.put("artwork_url_small", "VARCHAR(1000)");
		columnsAndTypes.put("itunes_release_date", "DATETIME");
		columnsAndTypes.put("copyright", "VARCHAR(4000)");
		columnsAndTypes.put("description", "LONGTEXT");
		columnsAndTypes.put("version", "VARCHAR(100)");
		columnsAndTypes.put("itunes_version", "VARCHAR(100)");
		columnsAndTypes.put("download_size", "BIGINT");
		primaryKey = new String[] { "application_id" };
	}

	@Test
	public void testInitImport1() throws EPFDbException, SQLException {
		String expectedColumnsAndTypes = "`export_date` BIGINT, `application_id` INTEGER, `title` VARCHAR(1000), `recommended_age` VARCHAR(20), `artist_name` VARCHAR(1000), `seller_name` VARCHAR(1000), `company_url` VARCHAR(1000), `support_url` VARCHAR(1000), `view_url` VARCHAR(1000), `artwork_url_large` VARCHAR(1000), `artwork_url_small` VARCHAR(1000), `itunes_release_date` DATETIME, `copyright` VARCHAR(4000), `description` LONGTEXT, `version` VARCHAR(100), `itunes_version` VARCHAR(100), `download_size` BIGINT";
		String expectedTmpTableName = tablePrefix + tableName + "_tmp";
		String expectedTableName = tablePrefix + tableName;
		String expectedSQLDropTable = String.format(
				EPFDbWriterMySQL.DROP_TABLE_STMT, expectedTmpTableName);
		String expectedSQLCreateTable = String.format("CREATE TABLE %s (%s)",
				expectedTmpTableName, expectedColumnsAndTypes);
		String expectedSQLCheckTable = String.format(
				"SELECT * FROM `%s` LIMIT 1", expectedTableName);

		EasyMock.reset(connector);
		connector.getConnection();
		EasyMock.expectLastCall().andReturn(connection).times(3);
		EasyMock.replay(connector);
		EasyMock.reset(connection);
		connection.createStatement();
		EasyMock.expectLastCall().andReturn(statement).times(3);
		connection.close();
		EasyMock.expectLastCall().times(3);
		EasyMock.replay(connection);
		EasyMock.reset(statement);
		statement.executeQuery(expectedSQLCheckTable);
		EasyMock.expectLastCall().andReturn(resultSet).times(1);
		statement.execute(expectedSQLDropTable);
		EasyMock.expectLastCall().andReturn(true).times(1);
		statement.execute(expectedSQLCreateTable);
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(statement);
		resultSet.getMetaData();
		EasyMock.expectLastCall().andReturn(metaData).times(1);
		EasyMock.replay(resultSet);
		metaData.reset();

		long numberOfRows = 500000;
		dbWriter.initImport(EPFExportType.FULL, tableName, columnsAndTypes,
				numberOfRows);

		EasyMock.verify(connector);
		EasyMock.verify(connection);
		EasyMock.verify(statement);
		EasyMock.verify(resultSet);
		Assert.assertTrue(
				String.format(
						"Invalid number of calls to getColumnCount(), expected %d, actual %d",
						columnsAndTypes.size() + 1,
						metaData.getColumnCountCount),
				columnsAndTypes.size() + 1 == metaData.getColumnCountCount());
		Assert.assertTrue(
				String.format(
						"Invalid number of calls to getColumnName(), expected %d, actual %d",
						columnsAndTypes.size(), metaData.getColumnNameCount),
				columnsAndTypes.size() == metaData.getColumnNameCount());
	}

	@Test
	public void testInitImport2() throws EPFDbException, SQLException {
		String expectedColumnsAndTypes = "`export_date` BIGINT, `application_id` INTEGER, `title` VARCHAR(1000), `recommended_age` VARCHAR(20), `artist_name` VARCHAR(1000), `seller_name` VARCHAR(1000), `company_url` VARCHAR(1000), `support_url` VARCHAR(1000), `view_url` VARCHAR(1000), `artwork_url_large` VARCHAR(1000), `artwork_url_small` VARCHAR(1000), `itunes_release_date` DATETIME, `copyright` VARCHAR(4000), `description` LONGTEXT, `version` VARCHAR(100), `itunes_version` VARCHAR(100), `download_size` BIGINT";
		String expectedTmpTableName = tablePrefix + tableName + "_tmp";
		String expectedTableName = tablePrefix + tableName;
		String expectedSQLDropTable = String.format(
				EPFDbWriterMySQL.DROP_TABLE_STMT, expectedTmpTableName);
		String expectedSQLCreateTable = String.format("CREATE TABLE %s (%s)",
				expectedTmpTableName, expectedColumnsAndTypes);
		String expectedSQLCheckTable = String.format(
				"SELECT * FROM `%s` LIMIT 1", expectedTableName);

		EasyMock.reset(connector);
		connector.getConnection();
		EasyMock.expectLastCall().andReturn(connection).times(3);
		EasyMock.replay(connector);
		EasyMock.reset(connection);
		connection.createStatement();
		EasyMock.expectLastCall().andReturn(statement).times(3);
		connection.close();
		EasyMock.expectLastCall().times(3);
		EasyMock.replay(connection);
		EasyMock.reset(statement);
		statement.executeQuery(expectedSQLCheckTable);
		EasyMock.expectLastCall().andReturn(resultSet).times(1);
		statement.execute(expectedSQLDropTable);
		EasyMock.expectLastCall().andReturn(true).times(1);
		statement.execute(expectedSQLCreateTable);
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(statement);
		resultSet.getMetaData();
		EasyMock.expectLastCall().andReturn(metaData).times(1);
		EasyMock.replay(resultSet);
		metaData.reset();

		long numberOfRows = 499999;
		dbWriter.initImport(EPFExportType.INCREMENTAL, tableName,
				columnsAndTypes, numberOfRows);

		EasyMock.verify(connector);
		EasyMock.verify(connection);
		EasyMock.verify(statement);
		EasyMock.verify(resultSet);
		Assert.assertTrue(
				String.format(
						"Invalid number of calls to getColumnCount(), expected %d, actual %d",
						columnsAndTypes.size() + 1,
						metaData.getColumnCountCount),
				columnsAndTypes.size() + 1 == metaData.getColumnCountCount());
		Assert.assertTrue(
				String.format(
						"Invalid number of calls to getColumnName(), expected %d, actual %d",
						columnsAndTypes.size(), metaData.getColumnNameCount),
				columnsAndTypes.size() == metaData.getColumnNameCount());
	}

	@Test
	public void testInitImport3() throws EPFDbException, SQLException {
		String expectedColumnsAndTypes = "`export_date` BIGINT, `application_id` INTEGER, `title` VARCHAR(1000), `recommended_age` VARCHAR(20), `artist_name` VARCHAR(1000), `seller_name` VARCHAR(1000), `company_url` VARCHAR(1000), `support_url` VARCHAR(1000), `view_url` VARCHAR(1000), `artwork_url_large` VARCHAR(1000), `artwork_url_small` VARCHAR(1000), `itunes_release_date` DATETIME, `copyright` VARCHAR(4000), `description` LONGTEXT, `version` VARCHAR(100), `itunes_version` VARCHAR(100), `download_size` BIGINT";
		String expectedTmpTableName = tablePrefix + tableName + "_tmp";
		String expectedUncTableName = tablePrefix + tableName + "_unc";
		String expectedTableName = tablePrefix + tableName;
		String expectedSQLDropTable = String.format(
				EPFDbWriterMySQL.DROP_TABLE_STMT, expectedTmpTableName);
		String expectedSQLDropTable2 = String.format(
				EPFDbWriterMySQL.DROP_TABLE_STMT, expectedUncTableName);
		String expectedSQLCreateTable = String.format("CREATE TABLE %s (%s)",
				expectedTmpTableName, expectedColumnsAndTypes);
		String expectedSQLCheckTable = String.format(
				"SELECT * FROM `%s` LIMIT 1", expectedTableName);

		EasyMock.reset(connector);
		connector.getConnection();
		EasyMock.expectLastCall().andReturn(connection).times(4);
		EasyMock.replay(connector);
		EasyMock.reset(connection);
		connection.createStatement();
		EasyMock.expectLastCall().andReturn(statement).times(4);
		connection.close();
		EasyMock.expectLastCall().times(4);
		EasyMock.replay(connection);
		EasyMock.reset(statement);
		statement.executeQuery(expectedSQLCheckTable);
		EasyMock.expectLastCall().andReturn(resultSet).times(1);
		statement.execute(expectedSQLDropTable);
		EasyMock.expectLastCall().andReturn(true).times(1);
		statement.execute(expectedSQLDropTable2);
		EasyMock.expectLastCall().andReturn(true).times(1);
		statement.execute(expectedSQLCreateTable);
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(statement);
		resultSet.getMetaData();
		EasyMock.expectLastCall().andReturn(metaData).times(1);
		EasyMock.replay(resultSet);
		metaData.reset();

		long numberOfRows = 500000;
		dbWriter.initImport(EPFExportType.INCREMENTAL, tableName,
				columnsAndTypes, numberOfRows);

		EasyMock.verify(connector);
		EasyMock.verify(connection);
		EasyMock.verify(statement);
		EasyMock.verify(resultSet);
		Assert.assertTrue(
				String.format(
						"Invalid number of calls to getColumnCount(), expected %d, actual %d",
						columnsAndTypes.size() + 1,
						metaData.getColumnCountCount),
				columnsAndTypes.size() + 1 == metaData.getColumnCountCount());
		Assert.assertTrue(
				String.format(
						"Invalid number of calls to getColumnName(), expected %d, actual %d",
						columnsAndTypes.size(), metaData.getColumnNameCount),
				columnsAndTypes.size() == metaData.getColumnNameCount());
	}

	@Test
	public void testSetPrimaryKey() throws SQLException, EPFDbException {
		String expectedTableName = tablePrefix + tableName + "_tmp";

		String expectedPrimaryKey = "";
		for (int i = 0; i < primaryKey.length; i++) {
			expectedPrimaryKey += "`" + primaryKey[i] + "`";
			if ((i + 1) < primaryKey.length) {
				expectedPrimaryKey += ",";
			}
		}

		String expectedSQLSetPrimaryKey = String.format(
				EPFDbWriterMySQL.PRIMARY_KEY_STMT, expectedTableName,
				expectedPrimaryKey);

		EasyMock.reset(connector);
		connector.getConnection();
		EasyMock.expectLastCall().andReturn(connection).times(1);
		EasyMock.replay(connector);
		EasyMock.reset(connection);
		connection.createStatement();
		EasyMock.expectLastCall().andReturn(statement).times(1);
		connection.close();
		EasyMock.expectLastCall().times(1);
		EasyMock.replay(connection);
		EasyMock.reset(statement);
		statement.execute(expectedSQLSetPrimaryKey);
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(statement);

		dbWriter.setPrimaryKey(expectedTableName, primaryKey);

		EasyMock.verify(connector);
		EasyMock.verify(connection);
		EasyMock.verify(statement);
	}

	@Test
	public void testImportFull() throws SQLException, EPFDbException {
		String expectedColumnsAndTypes = "`export_date` BIGINT, `application_id` INTEGER, `title` VARCHAR(1000), `recommended_age` VARCHAR(20), `artist_name` VARCHAR(1000), `seller_name` VARCHAR(1000), `company_url` VARCHAR(1000), `support_url` VARCHAR(1000), `view_url` VARCHAR(1000), `artwork_url_large` VARCHAR(1000), `artwork_url_small` VARCHAR(1000), `itunes_release_date` DATETIME, `copyright` VARCHAR(4000), `description` LONGTEXT, `version` VARCHAR(100), `itunes_version` VARCHAR(100), `download_size` BIGINT";
		String expectedTableName = tablePrefix + tableName;
		String expectedTmpTableName = tablePrefix + tableName + "_tmp";
		String expectedSQLDropTable = String.format(
				EPFDbWriterMySQL.DROP_TABLE_STMT, expectedTmpTableName);
		String expectedSQLCreateTable = String.format("CREATE TABLE %s (%s)",
				expectedTmpTableName, expectedColumnsAndTypes);
		String expectedSQLCheckTable = String.format(
				"SELECT * FROM `%s` LIMIT 1", expectedTableName);

		EasyMock.reset(connection);
		connection.createStatement();
		EasyMock.expectLastCall().andReturn(statement).times(4);
		connection.close();
		EasyMock.expectLastCall().anyTimes();
		EasyMock.replay(connection);
		
		EasyMock.reset(connector);
		connector.getConnection();
		EasyMock.expectLastCall().andReturn(connection).times(4);
		EasyMock.replay(connector);
		
		resultSet.getMetaData();
		EasyMock.expectLastCall().andReturn(metaData).times(1);
		EasyMock.replay(resultSet);
		
		int expectedRows  = 200;
		String expectedBlockInsertSQL;
		String expectedBlockInsertSQLFinalize;
		String expectedInsertColumns = getInsertColumns();
		String expectedInsertValues = "";
		
		int i;
		for (i = 0; i < expectedRows; i++) {
			expectedInsertValues += "(" + getInsertValues(i) + ")";
			if (i + 1 < expectedRows) {
				expectedInsertValues += ",";
			}
		}
		
		expectedBlockInsertSQL = String.format(EPFDbWriterMySQL.INSERT_SQL_STMT,"INSERT",expectedTmpTableName,expectedInsertColumns,expectedInsertValues);
		expectedInsertValues = "(" + getInsertValues(i) + ")";
		expectedBlockInsertSQLFinalize = String.format(EPFDbWriterMySQL.INSERT_SQL_STMT,"INSERT",expectedTmpTableName,expectedInsertColumns,expectedInsertValues);
		
		EasyMock.reset(statement);
		statement.executeQuery(expectedSQLCheckTable);
		EasyMock.expectLastCall().andReturn(resultSet).times(1);
		statement.execute(expectedSQLDropTable);
		EasyMock.expectLastCall().andReturn(true).times(1);
		statement.execute(expectedSQLCreateTable);
		EasyMock.expectLastCall().andReturn(true).times(1);
		statement.execute(expectedBlockInsertSQL);
		EasyMock.expectLastCall().andReturn(true).times(1);
		statement.execute(expectedBlockInsertSQLFinalize);
		EasyMock.expectLastCall().andReturn(true).anyTimes();
		EasyMock.replay(statement);
		
		metaData.reset();

		long numberOfRows = 500000;
		dbWriter.initImport(EPFExportType.FULL, tableName, columnsAndTypes,
				numberOfRows);

		for (i = 0; i < expectedRows + 1; i++) {
			dbWriter.insertRow(getInsertData(i));
		}
		
		dbWriter.finalizeImport();

		EasyMock.verify(connector);
		EasyMock.verify(connection);
		EasyMock.verify(statement);
		EasyMock.verify(resultSet);
		Assert.assertTrue(
				String.format(
						"Invalid number of calls to getColumnCount(), expected %d, actual %d",
						columnsAndTypes.size() + 1,
						metaData.getColumnCountCount),
				columnsAndTypes.size() + 1 == metaData.getColumnCountCount());
		Assert.assertTrue(
				String.format(
						"Invalid number of calls to getColumnName(), expected %d, actual %d",
						columnsAndTypes.size(), metaData.getColumnNameCount),
				columnsAndTypes.size() == metaData.getColumnNameCount());
	}

	@Test
	public void testImportIncrementalAppend() throws SQLException, EPFDbException {
		String expectedColumnsAndTypes = "`export_date` BIGINT, `application_id` INTEGER, `title` VARCHAR(1000), `recommended_age` VARCHAR(20), `artist_name` VARCHAR(1000), `seller_name` VARCHAR(1000), `company_url` VARCHAR(1000), `support_url` VARCHAR(1000), `view_url` VARCHAR(1000), `artwork_url_large` VARCHAR(1000), `artwork_url_small` VARCHAR(1000), `itunes_release_date` DATETIME, `copyright` VARCHAR(4000), `description` LONGTEXT, `version` VARCHAR(100), `itunes_version` VARCHAR(100), `download_size` BIGINT";
		String expectedTableName = tablePrefix + tableName;
		String expectedTmpTableName = tablePrefix + tableName + "_tmp";
		String expectedSQLDropTable = String.format(
				EPFDbWriterMySQL.DROP_TABLE_STMT, expectedTmpTableName);
		String expectedSQLCreateTable = String.format("CREATE TABLE %s (%s)",
				expectedTmpTableName, expectedColumnsAndTypes);
		String expectedSQLCheckTable = String.format(
				"SELECT * FROM `%s` LIMIT 1", expectedTableName);

		EasyMock.reset(connection);
		connection.createStatement();
		EasyMock.expectLastCall().andReturn(statement).times(4);
		connection.close();
		EasyMock.expectLastCall().anyTimes();
		EasyMock.replay(connection);
		
		EasyMock.reset(connector);
		connector.getConnection();
		EasyMock.expectLastCall().andReturn(connection).times(4);
		EasyMock.replay(connector);
		
		resultSet.getMetaData();
		EasyMock.expectLastCall().andReturn(metaData).times(1);
		EasyMock.replay(resultSet);
		
		int expectedRows  = 200;
		String expectedInsertColumns = getInsertColumns();
		String expectedInsertValues = "";
		
		int i;
		for (i = 0; i < expectedRows; i++) {
			expectedInsertValues += "(" + getInsertValues(i) + ")";
			if (i + 1 < expectedRows) {
				expectedInsertValues += ",";
			}
		}
		
		String expectedBlockInsertSQL = String.format(EPFDbWriterMySQL.INSERT_SQL_STMT,"REPLACE",expectedTmpTableName,expectedInsertColumns,expectedInsertValues);
		expectedInsertValues += "(" + getInsertValues(i) + ")";
		String expectedBlockInsertSQLFinalize = String.format(EPFDbWriterMySQL.INSERT_SQL_STMT,"REPLACE",expectedTmpTableName,expectedInsertColumns,expectedInsertValues);

		EasyMock.reset(statement);
		statement.executeQuery(expectedSQLCheckTable);
		EasyMock.expectLastCall().andReturn(resultSet).times(1);
		statement.execute(expectedSQLDropTable);
		EasyMock.expectLastCall().andReturn(true).times(1);
		statement.execute(expectedSQLCreateTable);
		EasyMock.expectLastCall().andReturn(true).times(1);
		statement.execute(expectedBlockInsertSQL);
		EasyMock.expectLastCall().andReturn(true).times(1);
		statement.execute(expectedBlockInsertSQLFinalize);
		EasyMock.expectLastCall().andReturn(true).anyTimes();
		EasyMock.replay(statement);
		
		metaData.reset();

		long numberOfRows = 499999;
		dbWriter.initImport(EPFExportType.INCREMENTAL, tableName, columnsAndTypes,
				numberOfRows);

		for (i = 0; i < expectedRows + 1; i++) {
			dbWriter.insertRow(getInsertData(i));
		}
		
		dbWriter.finalizeImport();

		EasyMock.verify(connector);
		EasyMock.verify(connection);
		EasyMock.verify(statement);
		EasyMock.verify(resultSet);
		Assert.assertTrue(
				String.format(
						"Invalid number of calls to getColumnCount(), expected %d, actual %d",
						columnsAndTypes.size() + 1,
						metaData.getColumnCountCount),
				columnsAndTypes.size() + 1 == metaData.getColumnCountCount());
		Assert.assertTrue(
				String.format(
						"Invalid number of calls to getColumnName(), expected %d, actual %d",
						columnsAndTypes.size(), metaData.getColumnNameCount),
				columnsAndTypes.size() == metaData.getColumnNameCount());
	}
	
	@Test
	public void testImportIncrementalMerge() throws SQLException, EPFDbException {
		String expectedColumnsAndTypes = "`export_date` BIGINT, `application_id` INTEGER, `title` VARCHAR(1000), `recommended_age` VARCHAR(20), `artist_name` VARCHAR(1000), `seller_name` VARCHAR(1000), `company_url` VARCHAR(1000), `support_url` VARCHAR(1000), `view_url` VARCHAR(1000), `artwork_url_large` VARCHAR(1000), `artwork_url_small` VARCHAR(1000), `itunes_release_date` DATETIME, `copyright` VARCHAR(4000), `description` LONGTEXT, `version` VARCHAR(100), `itunes_version` VARCHAR(100), `download_size` BIGINT";
		String expectedTableName = tablePrefix + tableName;
		String expectedTmpTableName = tablePrefix + tableName + "_tmp";
		String expectedUncTableName = tablePrefix + tableName + "_unc";
		String expectedSQLDropTable = String.format(
				EPFDbWriterMySQL.DROP_TABLE_STMT, expectedTmpTableName);
		String expectedSQLDropTable2 = String.format(
				EPFDbWriterMySQL.DROP_TABLE_STMT, expectedUncTableName);
		String expectedSQLCreateTable = String.format("CREATE TABLE %s (%s)",
				expectedTmpTableName, expectedColumnsAndTypes);
		String expectedSQLCheckTable = String.format(
				"SELECT * FROM `%s` LIMIT 1", expectedTableName);

		EasyMock.reset(connection);
		connection.createStatement();
		EasyMock.expectLastCall().andReturn(statement).times(5);
		connection.close();
		EasyMock.expectLastCall().anyTimes();
		EasyMock.replay(connection);
		
		EasyMock.reset(connector);
		connector.getConnection();
		EasyMock.expectLastCall().andReturn(connection).times(5);
		EasyMock.replay(connector);
		
		resultSet.getMetaData();
		EasyMock.expectLastCall().andReturn(metaData).times(1);
		EasyMock.replay(resultSet);
		
		int expectedRows  = 200;
		String expectedInsertColumns = getInsertColumns();
		String expectedInsertValues = "";
		
		int i;
		for (i = 0; i < expectedRows; i++) {
			expectedInsertValues += "(" + getInsertValues(i) + ")";
			if (i + 1 < expectedRows) {
				expectedInsertValues += ",";
			}
		}
		
		String expectedBlockInsertSQL = String.format(EPFDbWriterMySQL.INSERT_SQL_STMT,"INSERT",expectedTmpTableName,expectedInsertColumns,expectedInsertValues);
		expectedInsertValues += "(" + getInsertValues(i) + ")";
		String expectedBlockInsertSQLFinalize = String.format(EPFDbWriterMySQL.INSERT_SQL_STMT,"INSERT",expectedTmpTableName,expectedInsertColumns,expectedInsertValues);

		EasyMock.reset(statement);
		statement.executeQuery(expectedSQLCheckTable);
		EasyMock.expectLastCall().andReturn(resultSet).times(1);
		statement.execute(expectedSQLDropTable);
		EasyMock.expectLastCall().andReturn(true).times(1);
		statement.execute(expectedSQLDropTable2);
		EasyMock.expectLastCall().andReturn(true).times(1);
		statement.execute(expectedSQLCreateTable);
		EasyMock.expectLastCall().andReturn(true).times(1);
		statement.execute(expectedBlockInsertSQL);
		EasyMock.expectLastCall().andReturn(true).times(1);
		statement.execute(expectedBlockInsertSQLFinalize);
		EasyMock.expectLastCall().andReturn(true).anyTimes();
		EasyMock.replay(statement);
		
		metaData.reset();

		long numberOfRows = 500000;
		dbWriter.initImport(EPFExportType.INCREMENTAL, tableName, columnsAndTypes,
				numberOfRows);

		for (i = 0; i < expectedRows + 1; i++) {
			dbWriter.insertRow(getInsertData(i));
		}
		
		dbWriter.finalizeImport();

		EasyMock.verify(connector);
		EasyMock.verify(connection);
		EasyMock.verify(statement);
		EasyMock.verify(resultSet);
		Assert.assertTrue(
				String.format(
						"Invalid number of calls to getColumnCount(), expected %d, actual %d",
						columnsAndTypes.size() + 1,
						metaData.getColumnCountCount),
				columnsAndTypes.size() + 1 == metaData.getColumnCountCount());
		Assert.assertTrue(
				String.format(
						"Invalid number of calls to getColumnName(), expected %d, actual %d",
						columnsAndTypes.size(), metaData.getColumnNameCount),
				columnsAndTypes.size() == metaData.getColumnNameCount());
	}
	
	public String getInsertColumns() {
		Object[] columnNames = columnsAndTypes.keySet().toArray();
		String columns = "";
		
		for (int i = 0; i < columnNames.length; i++) {
			columns += "`" + columnNames[i] + "`";
			if (i + 1 < columnNames.length) {
				columns += ",";
			}
		}
		
		return columns;
	}
	
	public String getInsertValues(int rowNumber) {
		String[] rowData = getInsertData(rowNumber);
		Object[] columnTypes = columnsAndTypes.values().toArray();
		String values = ""; 
		
		for (int i = 0; i < rowData.length; i++) {
			if (EPFDbWriterMySQL.UNQUOTED_TYPES.contains(columnTypes[i])) {
				values += rowData[i];
			} else {
				values += "'" + rowData[i] + "'";
			}
			if (i + 1 < rowData.length) {
				values += ",";
			}
		}
		
		return values;
	}

	public String[] getInsertData(int rowNumber) {
		String[] columnData = new String[17];
		// Primary Key
		columnData[2 - 1] = String.valueOf(rowNumber); // columnsAndTypes.put("application_id",
														// "INTEGER");

		// Integers
		columnData[1 - 1] = "1"; // columnsAndTypes.put("export_date",
									// "BIGINT");
		columnData[17 - 1] = "17"; // columnsAndTypes.put("download_size",
									// "BIGINT");

		// Date Time
		columnData[12 - 1] = "2012-09-21 08:01:02"; // columnsAndTypes.put("itunes_release_date",
													// "DATETIME");

		// Strings
		columnData[3 - 1] = "title"; // columnsAndTypes.put("title",
										// "VARCHAR(1000)");
		columnData[4 - 1] = "9+"; // columnsAndTypes.put("recommended_age",
									// "VARCHAR(20)");
		columnData[5 - 1] = "artist name"; // columnsAndTypes.put("artist_name",
											// "VARCHAR(1000)");
		columnData[6 - 1] = "seller name"; // columnsAndTypes.put("seller_name",
											// "VARCHAR(1000)");
		columnData[7 - 1] = "http://www.somecompany.com"; // columnsAndTypes.put("company_url",
															// "VARCHAR(1000)");
		columnData[8 - 1] = "http://www.somecompany.com"; // columnsAndTypes.put("support_url",
															// "VARCHAR(1000)");
		columnData[9 - 1] = "http://www.somecompany.com"; // columnsAndTypes.put("view_url",
															// "VARCHAR(1000)");
		columnData[10 - 1] = "http://www.somecompany.com"; // columnsAndTypes.put("artwork_url_large",
															// "VARCHAR(1000)");
		columnData[11 - 1] = "http://www.somecompany.com"; // columnsAndTypes.put("artwork_url_small",
															// "VARCHAR(1000)");
		columnData[13 - 1] = "2012 Some Company, Inc"; // columnsAndTypes.put("copyright",
														// "VARCHAR(4000)");
		columnData[14 - 1] = "My Game"; // columnsAndTypes.put("description",
										// "LONGTEXT");
		columnData[15 - 1] = "1.0"; // columnsAndTypes.put("version",
									// "VARCHAR(100)");
		columnData[16 - 1] = "v12341234"; // columnsAndTypes.put("itunes_version",
											// "VARCHAR(100)");
		return columnData;
	}

	@Test
	public void testFinalizeImport() {
		fail("Not yet implemented");
	}

	class MockMetaData implements ResultSetMetaData {

		int getColumnCountCount = 0;
		int getColumnNameCount = 0;

		public void reset() {
			getColumnCountCount = 0;
			getColumnNameCount = 0;
		}

		public int getColumnCountCount() {
			return getColumnCountCount;
		}

		public int getColumnNameCount() {
			return getColumnNameCount;
		}

		@Override
		public boolean isWrapperFor(Class<?> arg0) throws SQLException {
			return false;
		}

		@Override
		public <T> T unwrap(Class<T> arg0) throws SQLException {
			return null;
		}

		@Override
		public String getCatalogName(int arg0) throws SQLException {
			return null;
		}

		@Override
		public String getColumnClassName(int arg0) throws SQLException {
			return null;
		}

		@Override
		public int getColumnCount() throws SQLException {
			getColumnCountCount++;
			return columnsAndTypes.size();
		}

		@Override
		public int getColumnDisplaySize(int arg0) throws SQLException {
			return 0;
		}

		@Override
		public String getColumnLabel(int arg0) throws SQLException {
			return null;
		}

		@Override
		public String getColumnName(int arg0) throws SQLException {
			getColumnNameCount++;
			return (String) columnsAndTypes.keySet().toArray()[arg0];
		}

		@Override
		public int getColumnType(int arg0) throws SQLException {
			return 0;
		}

		@Override
		public String getColumnTypeName(int arg0) throws SQLException {
			return null;
		}

		@Override
		public int getPrecision(int arg0) throws SQLException {
			return 0;
		}

		@Override
		public int getScale(int arg0) throws SQLException {
			return 0;
		}

		@Override
		public String getSchemaName(int arg0) throws SQLException {
			return null;
		}

		@Override
		public String getTableName(int arg0) throws SQLException {
			return null;
		}

		@Override
		public boolean isAutoIncrement(int arg0) throws SQLException {
			return false;
		}

		@Override
		public boolean isCaseSensitive(int arg0) throws SQLException {
			return false;
		}

		@Override
		public boolean isCurrency(int arg0) throws SQLException {
			return false;
		}

		@Override
		public boolean isDefinitelyWritable(int arg0) throws SQLException {
			return false;
		}

		@Override
		public int isNullable(int arg0) throws SQLException {
			return 0;
		}

		@Override
		public boolean isReadOnly(int arg0) throws SQLException {
			return false;
		}

		@Override
		public boolean isSearchable(int arg0) throws SQLException {
			return false;
		}

		@Override
		public boolean isSigned(int arg0) throws SQLException {
			return false;
		}

		@Override
		public boolean isWritable(int arg0) throws SQLException {
			return false;
		}
	}
}

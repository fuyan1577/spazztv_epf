package com.spazztv.epf.adapter;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.spazztv.epf.EPFExportType;
import com.spazztv.epf.dao.EPFDbException;
import com.spazztv.epf.dao.EPFDbWriter;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ EPFDbWriterMySql.class, EPFDbWriterMySqlDao.class,
		EPFDbWriterMySqlStmt.class })
public class EPFDbWriterMySqlTest {

	private EPFDbWriter dbWriter;
	private EPFDbWriterMySqlDao mySqlDao;
	private EPFDbWriterMySqlStmt mySqlStmt;
	private String tablePrefix = "test_";
	private String tableName = "epftable";
	private LinkedHashMap<String, String> columnsAndTypes;
	private List<String> primaryKey;

	@Before
	public void setUp() throws Exception {

		mySqlDao = EasyMock.createMock(EPFDbWriterMySqlDao.class);
		mySqlStmt = EasyMock.createMock(EPFDbWriterMySqlStmt.class);

		PowerMock.expectNew(EPFDbWriterMySqlDao.class, EasyMock.anyObject())
				.andReturn(mySqlDao);
		PowerMock.replay(EPFDbWriterMySqlDao.class);
		PowerMock.expectNew(EPFDbWriterMySqlStmt.class).andReturn(mySqlStmt);
		PowerMock.replay(EPFDbWriterMySqlStmt.class);

		dbWriter = new EPFDbWriterMySql();
		dbWriter.setTablePrefix(tablePrefix);
		// ((EPFDbWriterMySql) dbWriter).setMySqlDao(mySqlDao);

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

		String[] keyColumns = new String[] { "application_id" };
		primaryKey = Arrays.asList(keyColumns);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testInitImport1() throws EPFDbException, SQLException {
		String expectedColumnsAndTypes = "`export_date` BIGINT, `application_id` INTEGER, `title` VARCHAR(1000), `recommended_age` VARCHAR(20), `artist_name` VARCHAR(1000), `seller_name` VARCHAR(1000), `company_url` VARCHAR(1000), `support_url` VARCHAR(1000), `view_url` VARCHAR(1000), `artwork_url_large` VARCHAR(1000), `artwork_url_small` VARCHAR(1000), `itunes_release_date` DATETIME, `copyright` VARCHAR(4000), `description` LONGTEXT, `version` VARCHAR(100), `itunes_version` VARCHAR(100), `download_size` BIGINT";
		String expectedTmpTableName = tablePrefix + tableName + "_tmp";
		String expectedSQLDropTable = String.format(
				EPFDbWriterMySqlStmt.DROP_TABLE_STMT, expectedTmpTableName);
		String expectedSQLCreateTable = String.format("CREATE TABLE %s (%s)",
				expectedTmpTableName, expectedColumnsAndTypes);

		SQLReturnStatus returnStatus = new SQLReturnStatus();
		returnStatus.setSuccess(true);

		EasyMock.reset(mySqlStmt);
		// mySqlStmt.setupColumnAndTypesMap(EasyMock.eq(columnsAndTypes),
		// EasyMock.isNull())
		mySqlStmt.setupColumnAndTypesMap(EasyMock.eq(columnsAndTypes),
				(List<String>) EasyMock.isNull());
		EasyMock.expectLastCall().andReturn(columnsAndTypes).times(1);
		mySqlStmt.dropTableStmt(expectedTmpTableName);
		EasyMock.expectLastCall().andReturn(expectedSQLDropTable).times(1);
		mySqlStmt.createTableStmt(expectedTmpTableName, columnsAndTypes);
		EasyMock.expectLastCall().andReturn(expectedSQLCreateTable).times(1);
		EasyMock.replay(mySqlStmt);

		EasyMock.reset(mySqlDao);
		mySqlDao.executeSQLStatement(
				(String) EasyMock.eq(expectedSQLDropTable),
				(List<List<String>>) EasyMock.isNull());
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(
				(String) EasyMock.eq(expectedSQLCreateTable),
				(List<List<String>>) EasyMock.isNull());
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		EasyMock.replay(mySqlDao);

		long numberOfRows = 500000;
		dbWriter.initImport(EPFExportType.FULL, tableName, columnsAndTypes,
				primaryKey, numberOfRows);

		EasyMock.verify(mySqlDao);
	}

	@Test
	public void testInitImport2() throws EPFDbException, SQLException {
		String expectedTableName = tablePrefix + tableName;

		SQLReturnStatus returnStatus = new SQLReturnStatus();
		returnStatus.setSuccess(true);

		List<String> expectedTableColumns = new ArrayList<String>();
		for (String columnName : columnsAndTypes.keySet()) {
			expectedTableColumns.add(columnName);
		}

		EasyMock.reset(mySqlStmt);
		mySqlStmt.setupColumnAndTypesMap(EasyMock.eq(columnsAndTypes),
				EasyMock.eq(expectedTableColumns));
		EasyMock.expectLastCall().andReturn(columnsAndTypes).times(1);
		EasyMock.replay(mySqlStmt);

		EasyMock.reset(mySqlDao);
		mySqlDao.isTableInDatabase(expectedTableName);
		EasyMock.expectLastCall().andReturn(true).times(1);
		mySqlDao.getTableColumns(expectedTableName);
		EasyMock.expectLastCall().andReturn(expectedTableColumns).times(1);
		EasyMock.replay(mySqlDao);

		long numberOfRows = 499999;
		dbWriter.initImport(EPFExportType.INCREMENTAL, tableName,
				columnsAndTypes, primaryKey, numberOfRows);

		EasyMock.verify(mySqlDao);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testInitImport3() throws EPFDbException, SQLException {
		String expectedColumnsAndTypes = "`export_date` BIGINT, `application_id` INTEGER, `title` VARCHAR(1000), `recommended_age` VARCHAR(20), `artist_name` VARCHAR(1000), `seller_name` VARCHAR(1000), `company_url` VARCHAR(1000), `support_url` VARCHAR(1000), `view_url` VARCHAR(1000), `artwork_url_large` VARCHAR(1000), `artwork_url_small` VARCHAR(1000), `itunes_release_date` DATETIME, `copyright` VARCHAR(4000), `description` LONGTEXT, `version` VARCHAR(100), `itunes_version` VARCHAR(100), `download_size` BIGINT";
		String expectedTmpTableName = tablePrefix + tableName + "_tmp";
		String expectedUncTableName = tablePrefix + tableName + "_unc";
		String expectedTableName = tablePrefix + tableName;
		String expectedSQLDropTable = String.format(
				EPFDbWriterMySqlStmt.DROP_TABLE_STMT, expectedTmpTableName);
		String expectedSQLDropTable2 = String.format(
				EPFDbWriterMySqlStmt.DROP_TABLE_STMT, expectedUncTableName);
		String expectedSQLCreateTable = String.format("CREATE TABLE %s (%s)",
				expectedTmpTableName, expectedColumnsAndTypes);

		SQLReturnStatus returnStatus = new SQLReturnStatus();
		returnStatus.setSuccess(true);

		List<String> expectedTableColumns = new ArrayList<String>();
		for (String columnName : columnsAndTypes.keySet()) {
			expectedTableColumns.add(columnName);
		}

		EasyMock.reset(mySqlStmt);
		// mySqlStmt.setupColumnAndTypesMap(EasyMock.eq(columnsAndTypes),
		// EasyMock.isNull())
		mySqlStmt.setupColumnAndTypesMap(EasyMock.eq(columnsAndTypes),
				EasyMock.eq(expectedTableColumns));
		EasyMock.expectLastCall().andReturn(columnsAndTypes).times(1);
		mySqlStmt.dropTableStmt(expectedTmpTableName);
		EasyMock.expectLastCall().andReturn(expectedSQLDropTable).times(1);
		mySqlStmt.dropTableStmt(expectedUncTableName);
		EasyMock.expectLastCall().andReturn(expectedSQLDropTable2).times(1);
		mySqlStmt.createTableStmt(expectedTmpTableName, columnsAndTypes);
		EasyMock.expectLastCall().andReturn(expectedSQLCreateTable).times(1);
		EasyMock.replay(mySqlStmt);

		EasyMock.reset(mySqlDao);
		mySqlDao.isTableInDatabase(expectedTableName);
		EasyMock.expectLastCall().andReturn(true).times(1);
		mySqlDao.executeSQLStatement(
				(String) EasyMock.eq(expectedSQLDropTable),
				(List<List<String>>) EasyMock.isNull());
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(
				(String) EasyMock.eq(expectedSQLDropTable2),
				(List<List<String>>) EasyMock.isNull());
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(
				(String) EasyMock.eq(expectedSQLCreateTable),
				(List<List<String>>) EasyMock.isNull());
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.getTableColumns(expectedTableName);
		EasyMock.expectLastCall().andReturn(expectedTableColumns).times(1);
		EasyMock.replay(mySqlDao);

		long numberOfRows = 500000;
		dbWriter.initImport(EPFExportType.INCREMENTAL, tableName,
				columnsAndTypes, primaryKey, numberOfRows);

		EasyMock.verify(mySqlDao);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testImportFull() throws SQLException, EPFDbException {
		String expectedColumnsAndTypes = "`export_date` BIGINT, `application_id` INTEGER, `title` VARCHAR(1000), `recommended_age` VARCHAR(20), `artist_name` VARCHAR(1000), `seller_name` VARCHAR(1000), `company_url` VARCHAR(1000), `support_url` VARCHAR(1000), `view_url` VARCHAR(1000), `artwork_url_large` VARCHAR(1000), `artwork_url_small` VARCHAR(1000), `itunes_release_date` DATETIME, `copyright` VARCHAR(4000), `description` LONGTEXT, `version` VARCHAR(100), `itunes_version` VARCHAR(100), `download_size` BIGINT";
		String expectedTableName = tablePrefix + tableName;
		String expectedTmpTableName = tablePrefix + tableName + "_tmp";
		String expectedSQLDropTable = String.format(
				EPFDbWriterMySqlStmt.DROP_TABLE_STMT, expectedTmpTableName);
		String expectedSQLDropTableOld = String.format(
				EPFDbWriterMySqlStmt.DROP_TABLE_STMT, expectedTableName
						+ "_old");
		String expectedSQLCreateTable = String.format("CREATE TABLE %s (%s)",
				expectedTmpTableName, expectedColumnsAndTypes);
		// String expectedSQLCheckTable = String.format(
		// "SELECT * FROM `%s` LIMIT 1", expectedTableName);
		String expectedSQLRenameTable = String.format(
				EPFDbWriterMySqlStmt.RENAME_TABLE_STMT, expectedTableName,
				expectedTableName + "_old");
		String expectedSQLRenameTable2 = String.format(
				EPFDbWriterMySqlStmt.RENAME_TABLE_STMT, expectedTmpTableName,
				expectedTableName);

		int expectedRows = 200;
		String expectedBlockInsertSQL;
		String expectedBlockInsertSQLFinalize;
		String expectedInsertColumns = getInsertColumns();
		List<List<String>> expectedInsertData = new ArrayList<List<String>>();
		List<List<String>> expectedInsertDataFinalize = new ArrayList<List<String>>();
		String expectedInsertPreparedValues = "";
		String expectedAlterTableSetPrimaryKeySQL = String.format(
				EPFDbWriterMySqlStmt.PRIMARY_KEY_STMT, expectedTmpTableName,
				primaryKey);

		StringBuffer buf = new StringBuffer();
		int i;
		for (i = 0; i < expectedRows; i++) {
			expectedInsertData.add(getInsertData(i));
			buf.append("(" + getInsertPreparedValues(i) + ")");
			if (i + 1 < expectedRows) {
				buf.append(",");
			}
		}

		expectedInsertPreparedValues = buf.toString();
		expectedBlockInsertSQL = String.format(
				EPFDbWriterMySqlStmt.INSERT_SQL_STMT, "INSERT",
				expectedTmpTableName, expectedInsertColumns,
				expectedInsertPreparedValues);

		expectedInsertPreparedValues = "(" + getInsertPreparedValues(i) + ")";
		expectedInsertDataFinalize.add(getInsertData(i));
		expectedBlockInsertSQLFinalize = String.format(
				EPFDbWriterMySqlStmt.INSERT_SQL_STMT, "INSERT",
				expectedTmpTableName, expectedInsertColumns,
				expectedInsertPreparedValues);

		SQLReturnStatus returnStatus = new SQLReturnStatus();
		returnStatus.setSuccess(true);

		EasyMock.reset(mySqlStmt);
		mySqlStmt.setupColumnAndTypesMap(EasyMock.eq(columnsAndTypes),
				(List<String>) EasyMock.isNull());
		EasyMock.expectLastCall().andReturn(columnsAndTypes).times(1);
		mySqlStmt.dropTableStmt(expectedTmpTableName);
		EasyMock.expectLastCall().andReturn(expectedSQLDropTable).times(1);
		mySqlStmt.dropTableStmt(expectedTableName + "_old");
		EasyMock.expectLastCall().andReturn(expectedSQLDropTableOld).times(2);
		mySqlStmt.createTableStmt(expectedTmpTableName, columnsAndTypes);
		EasyMock.expectLastCall().andReturn(expectedSQLCreateTable).times(1);
		mySqlStmt
				.renameTableStmt(expectedTableName, expectedTableName + "_old");
		EasyMock.expectLastCall().andReturn(expectedSQLRenameTable);
		mySqlStmt.renameTableStmt(expectedTmpTableName, expectedTableName);
		EasyMock.expectLastCall().andReturn(expectedSQLRenameTable2);
		mySqlStmt.insertRowStmt(EasyMock.eq(expectedTmpTableName),
				EasyMock.eq(columnsAndTypes),
				(List<List<String>>) EasyMock.anyObject(),
				(String) EasyMock.anyObject());
		EasyMock.expectLastCall().andReturn(expectedBlockInsertSQL).times(1);
		EasyMock.expectLastCall().andReturn(expectedBlockInsertSQLFinalize)
				.times(1);
		mySqlStmt.setPrimaryKeyStmt(expectedTableName + "_tmp", primaryKey);
		EasyMock.expectLastCall().andReturn(expectedAlterTableSetPrimaryKeySQL)
				.times(1);
		EasyMock.replay(mySqlStmt);

		EasyMock.reset(mySqlDao);
		mySqlDao.isTableInDatabase(expectedTableName);
		EasyMock.expectLastCall().andReturn(true).times(1);
		mySqlDao.executeSQLStatement(expectedSQLDropTable, null);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedSQLDropTableOld, null);
		EasyMock.expectLastCall().andReturn(returnStatus).times(2);
		mySqlDao.executeSQLStatement(expectedSQLCreateTable, null);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedSQLRenameTable, null);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedSQLRenameTable2, null);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedBlockInsertSQL, expectedInsertData);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedBlockInsertSQLFinalize, expectedInsertDataFinalize);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedAlterTableSetPrimaryKeySQL, null);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		EasyMock.replay(mySqlDao);

		long numberOfRows = 500000;
		dbWriter.initImport(EPFExportType.FULL, tableName, columnsAndTypes,
				primaryKey, numberOfRows);

		for (i = 0; i < expectedRows + 1; i++) {
			dbWriter.insertRow(getInsertData(i));
		}

		dbWriter.finalizeImport();

		EasyMock.verify(mySqlStmt);
		EasyMock.verify(mySqlDao);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testImportIncrementalAppend() throws SQLException,
			EPFDbException {
		String expectedTableName = tablePrefix + tableName;

		int expectedRows = 200;
		String expectedBlockInsertSQL;
		String expectedBlockInsertSQLFinalize;
		String expectedInsertColumns = getInsertColumns();
		String expectedInsertPreparedValues = "";
		List<List<String>> expectedInsertData = new ArrayList<List<String>>();
		List<List<String>> expectedInsertDataFinalize = new ArrayList<List<String>>();

		int i;
		for (i = 0; i < expectedRows; i++) {
			expectedInsertPreparedValues += "(" + getInsertPreparedValues(i) + ")";
			expectedInsertData.add(getInsertData(i));
			if (i + 1 < expectedRows) {
				expectedInsertPreparedValues += ",";
			}
		}
		
		expectedBlockInsertSQL = String.format(
				EPFDbWriterMySqlStmt.INSERT_SQL_STMT, "REPLACE",
				expectedTableName, expectedInsertColumns, expectedInsertPreparedValues);

		expectedInsertDataFinalize.add(getInsertData(i));
		expectedInsertPreparedValues = "(" + getInsertPreparedValues(i) + ")";
		expectedBlockInsertSQLFinalize = String.format(
				EPFDbWriterMySqlStmt.INSERT_SQL_STMT, "REPLACE",
				expectedTableName, expectedInsertColumns, expectedInsertPreparedValues);

		List<String> expectedTableColumns = new ArrayList<String>();
		for (String columnName : columnsAndTypes.keySet()) {
			expectedTableColumns.add(columnName);
		}

		SQLReturnStatus returnStatus = new SQLReturnStatus();
		returnStatus.setSuccess(true);

		EasyMock.reset(mySqlStmt);
		mySqlStmt.setupColumnAndTypesMap(EasyMock.eq(columnsAndTypes),
				EasyMock.eq(expectedTableColumns));
		EasyMock.expectLastCall().andReturn(columnsAndTypes).times(1);
		mySqlStmt.insertRowStmt(EasyMock.eq(expectedTableName),
				EasyMock.eq(columnsAndTypes),
				(List<List<String>>) EasyMock.anyObject(),
				(String) EasyMock.anyObject());
		EasyMock.expectLastCall().andReturn(expectedBlockInsertSQL).times(1);
		EasyMock.expectLastCall().andReturn(expectedBlockInsertSQLFinalize)
				.times(1);
		EasyMock.replay(mySqlStmt);

		EasyMock.reset(mySqlDao);
		mySqlDao.isTableInDatabase(expectedTableName);
		EasyMock.expectLastCall().andReturn(true).times(1);
		mySqlDao.executeSQLStatement(
				(String) EasyMock.eq(expectedBlockInsertSQL),
				(List<List<String>>) EasyMock.eq(expectedInsertData));
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(
				(String) EasyMock.eq(expectedBlockInsertSQLFinalize),
				(List<List<String>>) EasyMock.eq(expectedInsertDataFinalize));
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.getTableColumns(expectedTableName);
		EasyMock.expectLastCall().andReturn(expectedTableColumns).times(1);
		EasyMock.replay(mySqlDao);

		long numberOfRows = 499999;
		dbWriter.initImport(EPFExportType.INCREMENTAL, tableName,
				columnsAndTypes, primaryKey, numberOfRows);

		for (i = 0; i < expectedRows + 1; i++) {
			dbWriter.insertRow(getInsertData(i));
		}

		dbWriter.finalizeImport();

		EasyMock.verify(mySqlStmt);
		EasyMock.verify(mySqlDao);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testImportIncrementalMerge() throws SQLException,
			EPFDbException {
		String expectedColumnsAndTypes = "`export_date` BIGINT, `application_id` INTEGER, `title` VARCHAR(1000), `recommended_age` VARCHAR(20), `artist_name` VARCHAR(1000), `seller_name` VARCHAR(1000), `company_url` VARCHAR(1000), `support_url` VARCHAR(1000), `view_url` VARCHAR(1000), `artwork_url_large` VARCHAR(1000), `artwork_url_small` VARCHAR(1000), `itunes_release_date` DATETIME, `copyright` VARCHAR(4000), `description` LONGTEXT, `version` VARCHAR(100), `itunes_version` VARCHAR(100), `download_size` BIGINT";
		String expectedTableName = tablePrefix + tableName;
		String expectedTmpTableName = tablePrefix + tableName + "_tmp";
		String expectedUncTableName = tablePrefix + tableName + "_unc";
		String expectedSQLDropTable = String.format(
				EPFDbWriterMySqlStmt.DROP_TABLE_STMT, expectedTmpTableName);
		String expectedSQLDropTableUnc = String.format(
				EPFDbWriterMySqlStmt.DROP_TABLE_STMT, expectedUncTableName);
		String expectedSQLCreateTable = String.format("CREATE TABLE %s (%s)",
				expectedTmpTableName, expectedColumnsAndTypes);
		String expectedSQLDropTableOld = String.format(
				EPFDbWriterMySqlStmt.DROP_TABLE_STMT, expectedTableName
						+ "_old");
		String expectedSQLRenameTable = String.format(
				EPFDbWriterMySqlStmt.RENAME_TABLE_STMT, expectedTableName,
				expectedTableName + "_old");
		String expectedSQLRenameTableUnc = String.format(
				EPFDbWriterMySqlStmt.RENAME_TABLE_STMT, expectedUncTableName,
				expectedTableName);

		int expectedRows = 200;
		String expectedInsertColumns = getInsertColumns();
		String expectedInsertPreparedValues = "";
		List<List<String>> expectedInsertData = new ArrayList<List<String>>();
		List<List<String>> expectedInsertDataFinalize = new ArrayList<List<String>>();

		int i;
		for (i = 0; i < expectedRows; i++) {
			expectedInsertData.add(getInsertData(i));
			expectedInsertPreparedValues += "(" + getInsertPreparedValues(i) + ")";
			if (i + 1 < expectedRows) {
				expectedInsertPreparedValues += ",";
			}
		}

		// Create the insert statement for the first block of records
		String expectedBlockInsertSQL = String.format(
				EPFDbWriterMySqlStmt.INSERT_SQL_STMT, "INSERT",
				expectedTmpTableName, expectedInsertColumns,
				expectedInsertPreparedValues);

		String expectedAlterTableSetPrimaryKeySQL = String.format(
				EPFDbWriterMySqlStmt.PRIMARY_KEY_STMT, expectedUncTableName,
				primaryKey);

		// Use 1 more record to create the insert statement for the
		// finalizeImport() call
		expectedInsertDataFinalize.add(getInsertData(i));
		expectedInsertPreparedValues = "(" + getInsertPreparedValues(i) + ")";
		String expectedBlockInsertSQLFinalize = String.format(
				EPFDbWriterMySqlStmt.INSERT_SQL_STMT, "INSERT",
				expectedTmpTableName, expectedInsertColumns,
				expectedInsertPreparedValues);

		// Create the final union merge query
		String expectedUnionJoin = String.format(
				EPFDbWriterMySqlStmt.UNION_CREATE_WHERE, expectedTableName,
				expectedTmpTableName);

		for (String k : primaryKey) {
			expectedUnionJoin += " "
					+ String.format(EPFDbWriterMySqlStmt.UNION_CREATE_JOIN,
							expectedTableName, k, expectedTmpTableName, k);
		}
		String expectedSQLCreateTableUnc = String.format(
				EPFDbWriterMySqlStmt.UNION_CREATE_PT1, expectedUncTableName,
				expectedTmpTableName);
		expectedSQLCreateTableUnc += " "
				+ String.format(EPFDbWriterMySqlStmt.UNION_CREATE_PT2,
						expectedTableName, expectedTmpTableName,
						expectedUnionJoin);

		SQLReturnStatus returnStatus = new SQLReturnStatus();
		returnStatus.setSuccess(true);

		List<String> expectedTableColumns = new ArrayList<String>();
		for (String columnName : columnsAndTypes.keySet()) {
			expectedTableColumns.add(columnName);
		}

		EasyMock.reset(mySqlStmt);
		mySqlStmt.setupColumnAndTypesMap(EasyMock.eq(columnsAndTypes),
				EasyMock.eq(expectedTableColumns));
		EasyMock.expectLastCall().andReturn(columnsAndTypes).times(1);
		mySqlStmt.dropTableStmt(expectedTmpTableName);
		EasyMock.expectLastCall().andReturn(expectedSQLDropTable).times(2);
		mySqlStmt.dropTableStmt(expectedTableName + "_old");
		EasyMock.expectLastCall().andReturn(expectedSQLDropTableOld).times(2);
		mySqlStmt.dropTableStmt(expectedUncTableName);
		EasyMock.expectLastCall().andReturn(expectedSQLDropTableUnc).times(1);
		mySqlStmt.createTableStmt(expectedTmpTableName, columnsAndTypes);
		EasyMock.expectLastCall().andReturn(expectedSQLCreateTable).times(1);
		mySqlStmt
				.renameTableStmt(expectedTableName, expectedTableName + "_old");
		EasyMock.expectLastCall().andReturn(expectedSQLRenameTable);
		mySqlStmt.renameTableStmt(expectedUncTableName, expectedTableName);
		EasyMock.expectLastCall().andReturn(expectedSQLRenameTableUnc);
		mySqlStmt.insertRowStmt(EasyMock.eq(expectedTmpTableName),
				EasyMock.eq(columnsAndTypes),
				(List<List<String>>) EasyMock.anyObject(),
				(String) EasyMock.anyObject());
		EasyMock.expectLastCall().andReturn(expectedBlockInsertSQL).times(1);
		EasyMock.expectLastCall().andReturn(expectedBlockInsertSQLFinalize)
				.times(1);
		mySqlStmt.mergeTableStmt(expectedTableName, expectedTmpTableName,
				expectedUncTableName, primaryKey);
		EasyMock.expectLastCall().andReturn(expectedSQLCreateTableUnc).times(1);
		mySqlStmt.setPrimaryKeyStmt(expectedUncTableName, primaryKey);
		EasyMock.expectLastCall().andReturn(expectedAlterTableSetPrimaryKeySQL)
				.times(1);

		EasyMock.replay(mySqlStmt);

		EasyMock.reset(mySqlDao);
		mySqlDao.isTableInDatabase(expectedTableName);
		EasyMock.expectLastCall().andReturn(true).times(1);
		mySqlDao.executeSQLStatement(expectedSQLDropTable, null);
		EasyMock.expectLastCall().andReturn(returnStatus).times(2);
		mySqlDao.executeSQLStatement(expectedSQLDropTableOld, null);
		EasyMock.expectLastCall().andReturn(returnStatus).times(2);
		mySqlDao.executeSQLStatement(expectedSQLDropTableUnc, null);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedSQLCreateTable, null);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedSQLRenameTable, null);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedSQLRenameTableUnc, null);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedBlockInsertSQL, expectedInsertData);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedBlockInsertSQLFinalize, expectedInsertDataFinalize);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.getTableColumns(expectedTableName);
		EasyMock.expectLastCall().andReturn(expectedTableColumns).times(1);
		mySqlDao.isTableInDatabase(expectedTableName);
		EasyMock.expectLastCall().andReturn(true).times(1);
		mySqlDao.executeSQLStatement(expectedSQLCreateTableUnc, null);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedAlterTableSetPrimaryKeySQL, null);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		EasyMock.replay(mySqlDao);

		long numberOfRows = 500000;
		dbWriter.initImport(EPFExportType.INCREMENTAL, tableName,
				columnsAndTypes, primaryKey, numberOfRows);

		for (i = 0; i < expectedRows + 1; i++) {
			dbWriter.insertRow(getInsertData(i));
		}

		dbWriter.finalizeImport();

		EasyMock.verify(mySqlStmt);
		EasyMock.verify(mySqlDao);

		// Assert.assertTrue(
		// String.format(
		// "Invalid number of calls to getColumnCount(), expected %d, actual %d",
		// columnsAndTypes.size() + 1,
		// metaData.getColumnCountCount),
		// columnsAndTypes.size() + 1 == metaData.getColumnCountCount());
		// Assert.assertTrue(
		// String.format(
		// "Invalid number of calls to getColumnName(), expected %d, actual %d",
		// columnsAndTypes.size(), metaData.getColumnNameCount),
		// columnsAndTypes.size() == metaData.getColumnNameCount());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testExecuteSQLStatementWithRetry() throws EPFDbException {
		// Set up the test data elements
		String expectedColumnsAndTypes = "`export_date` BIGINT, `application_id` INTEGER, `title` VARCHAR(1000), `recommended_age` VARCHAR(20), `artist_name` VARCHAR(1000), `seller_name` VARCHAR(1000), `company_url` VARCHAR(1000), `support_url` VARCHAR(1000), `view_url` VARCHAR(1000), `artwork_url_large` VARCHAR(1000), `artwork_url_small` VARCHAR(1000), `itunes_release_date` DATETIME, `copyright` VARCHAR(4000), `description` LONGTEXT, `version` VARCHAR(100), `itunes_version` VARCHAR(100), `download_size` BIGINT";
		String expectedTableName = tablePrefix + tableName;
		String expectedTmpTableName = tablePrefix + tableName + "_tmp";
		String expectedSQLDropTable = String.format(
				EPFDbWriterMySqlStmt.DROP_TABLE_STMT, expectedTmpTableName);
		String expectedSQLDropTableOld = String.format(
				EPFDbWriterMySqlStmt.DROP_TABLE_STMT, expectedTableName
						+ "_old");
		String expectedSQLCreateTable = String.format("CREATE TABLE %s (%s)",
				expectedTmpTableName, expectedColumnsAndTypes);
		// String expectedSQLCheckTable = String.format(
		// "SELECT * FROM `%s` LIMIT 1", expectedTableName);
		String expectedSQLRenameTable = String.format(
				EPFDbWriterMySqlStmt.RENAME_TABLE_STMT, expectedTableName,
				expectedTableName + "_old");
		String expectedSQLRenameTable2 = String.format(
				EPFDbWriterMySqlStmt.RENAME_TABLE_STMT, expectedTmpTableName,
				expectedTableName);

		String expectedSQLSetPrimaryKey = String.format(
				EPFDbWriterMySqlStmt.PRIMARY_KEY_STMT, expectedTmpTableName,
				"application_id");

		String expectedInsertColumns = getInsertColumns();
		String expectedInsertPreparedValues = "";
		List<List<String>> expectedInsertDataFinalize = new ArrayList<List<String>>();

		int i = 0;
		expectedInsertPreparedValues += "(" + getInsertPreparedValues(i) + ")";
		expectedInsertDataFinalize.add(getInsertData(i));
		List<String> insertData = getInsertData(i);

		String expectedInsertCommand = "INSERT";

		String expectedBlockInsertSQLFinalize = String.format(
				EPFDbWriterMySqlStmt.INSERT_SQL_STMT, expectedInsertCommand,
				expectedTableName, expectedInsertColumns, expectedInsertPreparedValues);

		// Set up the mocks
		// Set up mySqlStmt mock
		EasyMock.reset(mySqlStmt);
		mySqlStmt.setupColumnAndTypesMap(EasyMock.eq(columnsAndTypes),
				(List<String>) EasyMock.isNull());
		EasyMock.expectLastCall().andReturn(columnsAndTypes).times(1);
		mySqlStmt.dropTableStmt(expectedTmpTableName);
		EasyMock.expectLastCall().andReturn(expectedSQLDropTable).times(1);
		mySqlStmt.dropTableStmt(expectedTableName + "_old");
		EasyMock.expectLastCall().andReturn(expectedSQLDropTableOld).times(2);
		mySqlStmt.createTableStmt(expectedTmpTableName, columnsAndTypes);
		EasyMock.expectLastCall().andReturn(expectedSQLCreateTable).times(1);
		mySqlStmt
				.renameTableStmt(expectedTableName, expectedTableName + "_old");
		EasyMock.expectLastCall().andReturn(expectedSQLRenameTable);
		mySqlStmt.renameTableStmt(expectedTmpTableName, expectedTableName);
		EasyMock.expectLastCall().andReturn(expectedSQLRenameTable2);

		mySqlStmt.insertRowStmt(expectedTmpTableName, columnsAndTypes,
				expectedInsertDataFinalize, expectedInsertCommand);
		EasyMock.expectLastCall().andReturn(expectedBlockInsertSQLFinalize)
				.times(1);

		mySqlStmt.setPrimaryKeyStmt(expectedTmpTableName, primaryKey);
		EasyMock.expectLastCall().andReturn(expectedSQLSetPrimaryKey).times(1);
		EasyMock.replay(mySqlStmt);

		SQLReturnStatus successReturnStatus = new SQLReturnStatus();
		successReturnStatus.setSuccess(true);

		SQLReturnStatus errorReturnStatus = new SQLReturnStatus();
		errorReturnStatus.setSuccess(false);

		errorReturnStatus.setSqlState("23000");
		errorReturnStatus.setSqlExceptionCode(1205);

		// Set up mySqlDao mock
		EasyMock.reset(mySqlDao);
		mySqlDao.executeSQLStatement(
				(String) EasyMock.eq(expectedSQLDropTable),
				(List<List<String>>) EasyMock.isNull());
		EasyMock.expectLastCall().andReturn(successReturnStatus).times(1);
		mySqlDao.executeSQLStatement(
				(String) EasyMock.eq(expectedSQLDropTableOld),
				(List<List<String>>) EasyMock.isNull());
		EasyMock.expectLastCall().andReturn(successReturnStatus).times(1);
		mySqlDao.executeSQLStatement(
				(String) EasyMock.eq(expectedSQLCreateTable),
				(List<List<String>>) EasyMock.isNull());
		EasyMock.expectLastCall().andReturn(successReturnStatus).times(1);
		mySqlDao.executeSQLStatement(
				(String) EasyMock.eq(expectedBlockInsertSQLFinalize),
				(List<List<String>>) EasyMock.eq(expectedInsertDataFinalize));
		EasyMock.expectLastCall().andReturn(errorReturnStatus).times(1);
		EasyMock.expectLastCall().andReturn(successReturnStatus).times(1);
		mySqlDao.executeSQLStatement(
				EasyMock.eq(EPFDbWriterMySql.UNLOCK_TABLES),
				(List<List<String>>) EasyMock.isNull());
		EasyMock.expectLastCall().andReturn(successReturnStatus).times(1);
		mySqlDao.executeSQLStatement(EasyMock.eq(expectedSQLSetPrimaryKey),
				(List<List<String>>) EasyMock.isNull());
		EasyMock.expectLastCall().andReturn(successReturnStatus).times(1);
		mySqlDao.executeSQLStatement(EasyMock.eq(expectedSQLDropTableOld),
				(List<List<String>>) EasyMock.isNull());
		EasyMock.expectLastCall().andReturn(successReturnStatus).times(1);
		mySqlDao.isTableInDatabase(expectedTableName);
		EasyMock.expectLastCall().andReturn(true).times(1);
		mySqlDao.executeSQLStatement(EasyMock.eq(expectedSQLRenameTable),
				(List<List<String>>) EasyMock.isNull());
		EasyMock.expectLastCall().andReturn(successReturnStatus).times(1);
		mySqlDao.executeSQLStatement(EasyMock.eq(expectedSQLRenameTable2),
				(List<List<String>>) EasyMock.isNull());
		EasyMock.expectLastCall().andReturn(successReturnStatus).times(1);
		EasyMock.replay(mySqlDao);

		// Test method call
		dbWriter.initImport(EPFExportType.FULL, tableName, columnsAndTypes,
				primaryKey, 1L);
		dbWriter.insertRow(insertData);
		dbWriter.finalizeImport();

		// Verify the mocks
		EasyMock.verify(mySqlStmt);
		EasyMock.verify(mySqlDao);
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
		List<String> rowData = getInsertData(rowNumber);
		StringBuffer values = new StringBuffer();

		for (int i = 0; i < rowData.size(); i++) {
			if (EPFDbWriterMySqlStmt.UNQUOTED_TYPES.contains(columnsAndTypes.get(i))) {
				values.append(rowData.get(i));
			} else {
				values.append("'" + rowData.get(i) + "'");
			}
			if (i + 1 < rowData.size()) {
				values.append(",");
			}
		}

		return values.toString();
	}

	public String getInsertPreparedValues(int rowNumber) {
		List<String> rowData = getInsertData(rowNumber);
		StringBuffer values = new StringBuffer();

		for (int i = 0; i < rowData.size(); i++) {
			values.append("?");
			if (i + 1 < rowData.size()) {
				values.append(",");
			}
		}

		return values.toString();
	}

	public List<String> getInsertData(int rowNumber) {
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
		return Arrays.asList(columnData);
	}
}

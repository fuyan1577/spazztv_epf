package com.spazzmania.epf.mysql;

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

import com.spazzmania.epf.dao.EPFDbException;
import com.spazzmania.epf.dao.EPFDbWriter;
import com.spazzmania.epf.dao.SQLReturnStatus;
import com.spazzmania.epf.importer.EPFExportType;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ EPFDbWriterMySql.class, EPFDbWriterMySqlDao.class,
		EPFDbWriterMySqlStmt.class })
public class EPFDbWriterMySqlTest {

	EPFDbWriter dbWriter;
	EPFDbWriterMySqlDao mySqlDao;
	EPFDbWriterMySqlStmt mySqlStmt;
	String tablePrefix = "test_";
	String tableName = "epftable";
	LinkedHashMap<String, String> columnsAndTypes;
	String[] primaryKey;

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
		primaryKey = new String[] { "application_id" };
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
		mySqlDao.executeSQLStatement((String) EasyMock.eq(expectedSQLDropTable));
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement((String) EasyMock
				.eq(expectedSQLCreateTable));
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		EasyMock.replay(mySqlDao);

		long numberOfRows = 500000;
		dbWriter.initImport(EPFExportType.FULL, tableName, columnsAndTypes,
				numberOfRows);

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
				columnsAndTypes, numberOfRows);

		EasyMock.verify(mySqlDao);
	}

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
		mySqlDao.executeSQLStatement((String) EasyMock.eq(expectedSQLDropTable));
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement((String) EasyMock
				.eq(expectedSQLDropTable2));
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement((String) EasyMock
				.eq(expectedSQLCreateTable));
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.getTableColumns(expectedTableName);
		EasyMock.expectLastCall().andReturn(expectedTableColumns).times(1);
		EasyMock.replay(mySqlDao);

		long numberOfRows = 500000;
		dbWriter.initImport(EPFExportType.INCREMENTAL, tableName,
				columnsAndTypes, numberOfRows);

		EasyMock.verify(mySqlDao);
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
				EPFDbWriterMySqlStmt.PRIMARY_KEY_STMT, expectedTableName,
				expectedPrimaryKey);

		SQLReturnStatus returnStatus = new SQLReturnStatus();
		returnStatus.setSuccess(true);

		EasyMock.reset(mySqlStmt);
		// mySqlStmt.setupColumnAndTypesMap(EasyMock.eq(columnsAndTypes),
		// EasyMock.isNull())
		mySqlStmt.setPrimaryKeyStmt(expectedTableName, primaryKey);
		EasyMock.expectLastCall().andReturn(expectedSQLSetPrimaryKey).times(1);
		EasyMock.replay(mySqlStmt);

		EasyMock.reset(mySqlDao);
		mySqlDao.executeSQLStatement((String) EasyMock
				.eq(expectedSQLSetPrimaryKey));
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		EasyMock.replay(mySqlDao);

		dbWriter.setPrimaryKey(expectedTableName, primaryKey);

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
		String expectedInsertValues = "";

		StringBuffer buf = new StringBuffer();
		int i;
		for (i = 0; i < expectedRows; i++) {
			buf.append("(" + getInsertValues(i) + ")");
			if (i + 1 < expectedRows) {
				buf.append(",");
			}
		}

		expectedInsertValues = buf.toString();

		expectedBlockInsertSQL = String.format(
				EPFDbWriterMySqlStmt.INSERT_SQL_STMT, "INSERT",
				expectedTmpTableName, expectedInsertColumns,
				expectedInsertValues);
		expectedInsertValues = "(" + getInsertValues(i) + ")";
		expectedBlockInsertSQLFinalize = String.format(
				EPFDbWriterMySqlStmt.INSERT_SQL_STMT, "INSERT",
				expectedTmpTableName, expectedInsertColumns,
				expectedInsertValues);

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
		mySqlStmt.insertRowStatement(EasyMock.eq(expectedTmpTableName),
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
		mySqlDao.executeSQLStatement(expectedSQLDropTable);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedSQLDropTableOld);
		EasyMock.expectLastCall().andReturn(returnStatus).times(2);
		mySqlDao.executeSQLStatement(expectedSQLCreateTable);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedSQLRenameTable);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedSQLRenameTable2);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedBlockInsertSQL);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedBlockInsertSQLFinalize);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		EasyMock.replay(mySqlDao);

		long numberOfRows = 500000;
		dbWriter.initImport(EPFExportType.FULL, tableName, columnsAndTypes,
				numberOfRows);

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
		String expectedInsertValues = "";

		int i;
		for (i = 0; i < expectedRows; i++) {
			expectedInsertValues += "(" + getInsertValues(i) + ")";
			if (i + 1 < expectedRows) {
				expectedInsertValues += ",";
			}
		}

		expectedBlockInsertSQL = String.format(
				EPFDbWriterMySqlStmt.INSERT_SQL_STMT, "REPLACE",
				expectedTableName, expectedInsertColumns, expectedInsertValues);
		expectedInsertValues = "(" + getInsertValues(i) + ")";
		expectedBlockInsertSQLFinalize = String.format(
				EPFDbWriterMySqlStmt.INSERT_SQL_STMT, "REPLACE",
				expectedTableName, expectedInsertColumns, expectedInsertValues);

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
		mySqlStmt.insertRowStatement(EasyMock.eq(expectedTableName),
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
		mySqlDao.executeSQLStatement((String) EasyMock
				.eq(expectedBlockInsertSQL));
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement((String) EasyMock
				.eq(expectedBlockInsertSQLFinalize));
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.getTableColumns(expectedTableName);
		EasyMock.expectLastCall().andReturn(expectedTableColumns).times(1);
		EasyMock.replay(mySqlDao);

		long numberOfRows = 499999;
		dbWriter.initImport(EPFExportType.INCREMENTAL, tableName,
				columnsAndTypes, numberOfRows);

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
		String expectedInsertValues = "";

		int i;
		for (i = 0; i < expectedRows; i++) {
			expectedInsertValues += "(" + getInsertValues(i) + ")";
			if (i + 1 < expectedRows) {
				expectedInsertValues += ",";
			}
		}

		// Create the insert statement for the first block of records
		String expectedBlockInsertSQL = String.format(
				EPFDbWriterMySqlStmt.INSERT_SQL_STMT, "INSERT",
				expectedTmpTableName, expectedInsertColumns,
				expectedInsertValues);

		// Use 1 more record to create the insert statement for the
		// finalizeImport() call
		expectedInsertValues = "(" + getInsertValues(i) + ")";
		String expectedBlockInsertSQLFinalize = String.format(
				EPFDbWriterMySqlStmt.INSERT_SQL_STMT, "INSERT",
				expectedTmpTableName, expectedInsertColumns,
				expectedInsertValues);

		// Create the final union merge query
		String expectedUnionJoin = String.format(
				EPFDbWriterMySqlStmt.UNION_CREATE_WHERE, expectedTableName,
				expectedTmpTableName);
		for (i = 0; i < primaryKey.length; i++) {
			expectedUnionJoin += " "
					+ String.format(EPFDbWriterMySqlStmt.UNION_CREATE_JOIN,
							expectedTableName, primaryKey[i],
							expectedTmpTableName, primaryKey[i]);
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

		String expectedPrimaryKey = "";
		for (i = 0; i < primaryKey.length; i++) {
			expectedPrimaryKey += "`" + primaryKey[i] + "`";
			if ((i + 1) < primaryKey.length) {
				expectedPrimaryKey += ",";
			}
		}

		String expectedSQLSetPrimaryKey = String.format(
				EPFDbWriterMySqlStmt.PRIMARY_KEY_STMT, expectedTableName,
				expectedPrimaryKey);

		EasyMock.reset(mySqlStmt);
		mySqlStmt.setupColumnAndTypesMap(EasyMock.eq(columnsAndTypes),
				EasyMock.eq(expectedTableColumns));
		EasyMock.expectLastCall().andReturn(columnsAndTypes).times(1);
		mySqlStmt.setPrimaryKeyStmt(expectedTableName, primaryKey);
		EasyMock.expectLastCall().andReturn(expectedSQLSetPrimaryKey).times(1);
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
		mySqlStmt.insertRowStatement(EasyMock.eq(expectedTmpTableName),
				EasyMock.eq(columnsAndTypes),
				(List<List<String>>) EasyMock.anyObject(),
				(String) EasyMock.anyObject());
		EasyMock.expectLastCall().andReturn(expectedBlockInsertSQL).times(1);
		EasyMock.expectLastCall().andReturn(expectedBlockInsertSQLFinalize)
				.times(1);
		mySqlStmt.mergeTableStmt(expectedTableName, expectedTmpTableName, expectedUncTableName, Arrays.asList(primaryKey));
		EasyMock.expectLastCall().andReturn(expectedSQLCreateTableUnc).times(1);
		EasyMock.replay(mySqlStmt);

		EasyMock.reset(mySqlDao);
		mySqlDao.isTableInDatabase(expectedTableName);
		EasyMock.expectLastCall().andReturn(true).times(1);
		mySqlDao.executeSQLStatement(expectedSQLDropTable);
		EasyMock.expectLastCall().andReturn(returnStatus).times(2);
		mySqlDao.executeSQLStatement(expectedSQLDropTableOld);
		EasyMock.expectLastCall().andReturn(returnStatus).times(2);
		mySqlDao.executeSQLStatement(expectedSQLDropTableUnc);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedSQLCreateTable);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedSQLRenameTable);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedSQLRenameTableUnc);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedBlockInsertSQL);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedBlockInsertSQLFinalize);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.executeSQLStatement(expectedSQLSetPrimaryKey);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		mySqlDao.getTableColumns(expectedTableName);
		EasyMock.expectLastCall().andReturn(expectedTableColumns).times(1);
		mySqlDao.isTableInDatabase(expectedTableName);
		EasyMock.expectLastCall().andReturn(true).times(1);
		mySqlDao.executeSQLStatement(expectedSQLCreateTableUnc);
		EasyMock.expectLastCall().andReturn(returnStatus).times(1);
		EasyMock.replay(mySqlDao);

		long numberOfRows = 500000;
		dbWriter.initImport(EPFExportType.INCREMENTAL, tableName,
				columnsAndTypes, numberOfRows);

		dbWriter.setPrimaryKey(expectedTableName, primaryKey);

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

	@Test
	public void testExecuteSQLStatementWithRetry() throws EPFDbException {
		String expectedTableName = tablePrefix + tableName + "_tmp";

		String expectedPrimaryKey = "";
		for (int i = 0; i < primaryKey.length; i++) {
			expectedPrimaryKey += "`" + primaryKey[i] + "`";
			if ((i + 1) < primaryKey.length) {
				expectedPrimaryKey += ",";
			}
		}

		String expectedSQLSetPrimaryKey = String.format(
				EPFDbWriterMySqlStmt.PRIMARY_KEY_STMT, expectedTableName,
				expectedPrimaryKey);

		SQLReturnStatus successReturnStatus = new SQLReturnStatus();
		successReturnStatus.setSuccess(true);

		SQLReturnStatus errorReturnStatus = new SQLReturnStatus();
		errorReturnStatus.setSuccess(false);

		errorReturnStatus.setSqlStateCode("23000");
		errorReturnStatus.setSqlExceptionCode(1205);

		EasyMock.reset(mySqlStmt);
		mySqlStmt.setPrimaryKeyStmt(expectedTableName, primaryKey);
		EasyMock.expectLastCall().andReturn(expectedSQLSetPrimaryKey).times(1);
		EasyMock.replay(mySqlStmt);
		
		EasyMock.reset(mySqlDao);
		mySqlDao.executeSQLStatement((String) EasyMock
				.eq(expectedSQLSetPrimaryKey));
		EasyMock.expectLastCall().andReturn(errorReturnStatus).times(1);
		EasyMock.expectLastCall().andReturn(successReturnStatus).times(1);
		mySqlDao.executeSQLStatement(EPFDbWriterMySql.UNLOCK_TABLES);
		EasyMock.expectLastCall().andReturn(successReturnStatus).times(1);
		EasyMock.replay(mySqlDao);

		dbWriter.setPrimaryKey(expectedTableName, primaryKey);

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
		String[] rowData = getInsertData(rowNumber);
		Object[] columnTypes = columnsAndTypes.values().toArray();
		StringBuffer values = new StringBuffer();

		for (int i = 0; i < rowData.length; i++) {
			if (EPFDbWriterMySql.UNQUOTED_TYPES.contains(columnTypes[i])) {
				values.append(rowData[i]);
			} else {
				values.append("'" + rowData[i] + "'");
			}
			if (i + 1 < rowData.length) {
				values.append(",");
			}
		}

		return values.toString();
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
}

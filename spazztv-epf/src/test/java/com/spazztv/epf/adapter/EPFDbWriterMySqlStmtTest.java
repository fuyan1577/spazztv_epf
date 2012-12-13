package com.spazztv.epf.adapter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class EPFDbWriterMySqlStmtTest {

	private EPFDbWriterMySqlStmt sqlStmt;
	private String tableName = "stmt_table";
	private LinkedHashMap<String, String> columnsAndTypes;
	private List<String> primaryKey;
	private String[] keyColumns;

	@Before
	public void setUp() throws Exception {
		sqlStmt = new EPFDbWriterMySqlStmt();
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

		keyColumns = new String[] { "application_id" };
		primaryKey = new ArrayList<String>();
		primaryKey.add("application_id");
	}

	@Test
	public void testDropTableStmt() {
		String expectedDropTableStmt = String.format(
				EPFDbWriterMySqlStmt.DROP_TABLE_STMT, tableName);

		String actualDropTableStmt = sqlStmt.dropTableStmt(tableName);

		Assert.assertTrue(String.format(
				"Unexpected drop table statement: Expecting %s, actual %s",
				expectedDropTableStmt, actualDropTableStmt),
				expectedDropTableStmt.equals(actualDropTableStmt));
	}

	@Test
	public void testCreateTableStmt() {

		String columnsToCreate = "";
		Iterator<Entry<String, String>> entrySet = columnsAndTypes.entrySet()
				.iterator();
		while (entrySet.hasNext()) {
			Entry<String, String> colNType = entrySet.next();
			columnsToCreate += "`" + colNType.getKey() + "` ";
			if (!colNType.getValue().equals("CLOB")) {
				columnsToCreate += colNType.getValue();
			} else {
				columnsToCreate += "LONGTEXT";
			}

			if (entrySet.hasNext()) {
				columnsToCreate += ", ";
			}
		}

		String expectedCreateTableStmt = String.format(
				EPFDbWriterMySqlStmt.CREATE_TABLE_STMT, tableName,
				columnsToCreate);

		String actualCreateTableStmt = sqlStmt.createTableStmt(tableName,
				columnsAndTypes);

		Assert.assertTrue(String.format(
				"Unexpected create table statement: Expecting %s, actual %s",
				expectedCreateTableStmt, actualCreateTableStmt),
				expectedCreateTableStmt.equals(actualCreateTableStmt));
	}

	@Test
	public void testSetupColumnAndTypesMap() {
		LinkedHashMap<String, String> expectedColumnsAndTypes = new LinkedHashMap<String, String>();
		List<String> expectedColumns = new ArrayList<String>();
		for (String column : columnsAndTypes.keySet()) {
			expectedColumns.add(column);
			expectedColumnsAndTypes.put(column, columnsAndTypes.get(column));
		}

		// Remove the last column for the test
		expectedColumnsAndTypes.remove(expectedColumns.get(expectedColumns
				.size() - 1));
		expectedColumns.remove(expectedColumns.size() - 1);

		LinkedHashMap<String, String> actualColumnsAndTypes = sqlStmt
				.setupColumnAndTypesMap(columnsAndTypes, expectedColumns);

		Assert.assertTrue(String.format(
				"Unexpected number of columns: Expecting %d, actual %d",
				expectedColumnsAndTypes.size(), actualColumnsAndTypes.size()),
				expectedColumnsAndTypes.size() == actualColumnsAndTypes.size());

		// Now with a null columnNames operand
		actualColumnsAndTypes = sqlStmt.setupColumnAndTypesMap(columnsAndTypes,
				null);

		Assert.assertTrue(String.format(
				"Unexpected number of columns: Expecting %d, actual %d",
				columnsAndTypes.size(), actualColumnsAndTypes.size()),
				columnsAndTypes.size() == actualColumnsAndTypes.size());
	}

	@Test
	public void testSetPrimaryKeyStmt() {

		String expectedPrimaryKeyColumns = "";

		for (int i = 0; i < keyColumns.length; i++) {
			expectedPrimaryKeyColumns += "`" + keyColumns[i] + "`";
			if (i + 1 < keyColumns.length) {
				expectedPrimaryKeyColumns += ",";
			}
		}

		String expectedPrimaryKeyStmt = String.format(
				EPFDbWriterMySqlStmt.PRIMARY_KEY_STMT, tableName,
				expectedPrimaryKeyColumns);

		String actualPrimaryKeyStmt = sqlStmt.setPrimaryKeyStmt(tableName,
				primaryKey);

		Assert.assertTrue(String.format(
				"Unexpected drop table statement: Expecting %s, actual %s",
				expectedPrimaryKeyStmt, actualPrimaryKeyStmt),
				expectedPrimaryKeyStmt.equals(actualPrimaryKeyStmt));
	}

	@Test
	public void testInsertRowStmt() {

		String insertCommand = "INSERT";
		List<List<String>> insertValues = new ArrayList<List<String>>();
		String expectedBlockInsertSQL;
		String expectedInsertColumns = getInsertColumns();
		String expectedInsertValues = "";

		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < 2; i++) {
			String[] row = getInsertData(i);
			if (buf.length() > 0) {
				buf.append(",");
			}
			buf.append("(" + getInsertValuesPreparedStatement(i) + ")");
			insertValues.add((List<String>) Arrays.asList(row));
		}

		expectedInsertValues = buf.toString();
		expectedBlockInsertSQL = String.format(
				EPFDbWriterMySqlStmt.INSERT_SQL_STMT, insertCommand, tableName,
				expectedInsertColumns, expectedInsertValues);

		String actualBlockInsertSQL = sqlStmt.insertRowStmt(tableName,
				columnsAndTypes, insertValues, insertCommand);

		Assert.assertTrue(String.format(
				"Invalid block insert statement: Expecting %s, actual %s",
				expectedBlockInsertSQL, actualBlockInsertSQL),
				expectedBlockInsertSQL.equals(actualBlockInsertSQL));
	}

	@Test
	public void testMergeTableStmt() {
		String incTableName = tableName + "_tmp";
		String unionTableName = tableName + "_unc";

		String mergeWhere = String.format(
				EPFDbWriterMySqlStmt.UNION_CREATE_WHERE, tableName,
				incTableName);

		Iterator<String> keyColumns = primaryKey.iterator();
		while (keyColumns.hasNext()) {
			String keyColumn = keyColumns.next();
			mergeWhere += " "
					+ String.format(EPFDbWriterMySqlStmt.UNION_CREATE_JOIN,
							tableName, keyColumn, incTableName, keyColumn);
		}

		String execptedMergeTableStmt = String.format(
				EPFDbWriterMySqlStmt.UNION_CREATE_PT1, unionTableName,
				incTableName);
		execptedMergeTableStmt += " "
				+ String.format(EPFDbWriterMySqlStmt.UNION_CREATE_PT2,
						tableName, incTableName, mergeWhere);

		String actualMergeTableStmt = sqlStmt.mergeTableStmt(tableName,
				incTableName, unionTableName, primaryKey);

		Assert.assertTrue(String.format(
				"Invalid merge table statement: Expecting %s, actual %s",
				execptedMergeTableStmt, actualMergeTableStmt),
				execptedMergeTableStmt.equals(actualMergeTableStmt));
	}

	@Test
	public void testRenameTableStmt() {
		String unionTableName = tableName + "_unc";

		String expectedRenameTableStmt = String.format(
				EPFDbWriterMySqlStmt.RENAME_TABLE_STMT, unionTableName,
				tableName);

		String actualRenameTableStmt = sqlStmt.renameTableStmt(unionTableName,
				tableName);

		Assert.assertTrue(String.format(
				"Invalid rename table statement: Expecting %s, actual %s",
				expectedRenameTableStmt, actualRenameTableStmt),
				expectedRenameTableStmt.equals(actualRenameTableStmt));
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

	public String getInsertValuesPreparedStatement(int rowNumber) {
		String[] rowData = getInsertData(rowNumber);
		StringBuffer values = new StringBuffer();

		for (int i = 0; i < rowData.length; i++) {
			values.append("?");
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

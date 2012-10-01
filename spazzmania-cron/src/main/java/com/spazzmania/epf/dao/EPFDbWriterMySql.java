package com.spazzmania.epf.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.mysql.jdbc.MysqlErrorNumbers;
import com.spazzmania.epf.importer.EPFExportType;

/**
 * A MySQL Implementation of the EPFDbWriter interface.
 * <p>
 * This implementation matches the original EPF Python scripts and creates a
 * MyISAM table based on the import file definition and data.
 * 
 * <p>
 * This has three modes of import with three very different modes of operation:
 * <ul>
 * <li/><b>Full</b> import of the file replacing any previous table and data of
 * the same name. Data is imported to a temporary table first and on successful
 * completion of the import, the original table is dropped and the temporary
 * table is renamed.
 * <li/><b>Append</b> import is an <i>Incremental</i> importing of records
 * appending them directly to the existing table. This method is done in
 * incremental imports of less than 500,000 records.
 * <p>
 * <i>Note:</i> If the table doesn't exist, the table is created before
 * appending records.
 * <li/><b>Merge</b> import is an <i>Incremental</i> importing of records where
 * the new data is imported to a temporary table, then a merge table is created
 * of the previous data and new data and, finally, the previous table is dropped
 * and the new union table is renamed. This technique is used when the
 * incremental import has 500,000 or more records.
 * </ul>
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFDbWriterMySql extends EPFDbWriter {

	public static String DROP_TABLE_STMT = "DROP TABLE IF EXISTS %s";
	public static String CREATE_TABLE_STMT = "CREATE TABLE %s (%s)";
	public static String RENAME_TABLE_STMT = "ALTER TABLE %s RENAME %s";
	public static String PRIMARY_KEY_STMT = "ALTER TABLE %s ADD CONSTRAINT PRIMARY KEY (%s)";
	public static String INSERT_SQL_STMT = "%s INTO %s (%s) VALUES %s";
	public static String TABLE_EXISTS_SQL = "SHOW TABLES";
	public static String UNLOCK_TABLES = "UNLOCK TABLES";

	public static String UNION_QUERY_PT1 = "CREATE TABLE %s IGNORE SELECT * FROM %s UNION ALL ";
	public static String UNION_QUERY_PT2 = "SELECT * FROM %s WHERE 0 = (SELECT COUNT(*) FROM %s %s";
	public static String UNION_QUERY_WHERE = "WHERE %s.export_date <= %s.export_date";
	public static String UNION_QUERY_JOIN = "AND %s.%s = %s.%s ";

	public static int EXECUTE_SQL_STATEMENT_RETRIES = 30;
	public static long MERGE_THRESHOLD = 500000;
	public static long INSERT_BUFFER_SIZE = 200;
	public static int MAX_SQL_ATTEMPTS = 3;

	private EPFDbWriterMySqlDao mySqlDao;

	public static Map<String, String> TRANSLATION_MAP = Collections
			.unmodifiableMap(new HashMap<String, String>() {
				private static final long serialVersionUID = 1L;
				{
					put("CLOB", "LONGTEXT");
				}
			});

	public static List<String> UNQUOTED_TYPES = Collections
			.unmodifiableList(new ArrayList<String>() {
				private static final long serialVersionUID = 1L;
				{
					add("INTEGER");
					add("INT");
					add("BIGINT");
					add("TINYINT");
				}
			});

	private String tableName;
	private String impTableName;
	private String uncTableName;
	private boolean[] quotedColumnType;

	private String columnNames;
	private Map<Integer, String> columnMap;
	private List<String> primaryKey;

	private int insertBufferCount = 0;
	String insertBuffer;

	private enum ProcessMode {
		IMPORT_RENAME, APPEND, MERGE_RENAME
	};

	private ProcessMode processMode;

	public EPFDbWriterMySql() {
		super();
	}

	public void setMySqlDao(EPFDbWriterMySqlDao mySqlDao) {
		this.mySqlDao = mySqlDao;
	}

	@Override
	public void initImport(EPFExportType exportType, String tableName,
			LinkedHashMap<String, String> columnsAndTypes, long numberOfRows)
			throws EPFDbException {

		this.tableName = getTablePrefix() + tableName;
		//Default method is to create a temporary table and append data
		this.impTableName = this.tableName + "_tmp";
		this.uncTableName = null;

		setupColumnMap(columnsAndTypes, exportType);

		if (exportType == EPFExportType.FULL) {
			processMode = ProcessMode.IMPORT_RENAME;
		} else {
			if (!mySqlDao.isTableInDatabase(this.tableName)) {
				throw new EPFDbException("Table not found - cannot append data");
			}
			if (numberOfRows < MERGE_THRESHOLD) {
				//Append directly to the existing table
				processMode = ProcessMode.APPEND;
				this.impTableName = this.tableName;
			} else {
				processMode = ProcessMode.MERGE_RENAME;
				uncTableName = this.tableName + "_unc";
				dropTable(uncTableName);
			}
		}

		if (processMode == ProcessMode.IMPORT_RENAME
				|| processMode == ProcessMode.MERGE_RENAME) {
			dropTable(impTableName);
			createTable(impTableName, columnsAndTypes);
		}
		
		insertBufferCount = 0;
	}

	private void dropTable(String tableName) throws EPFDbException {
		String sqlDrop = String.format(DROP_TABLE_STMT, tableName);
		SQLReturnStatus status;
		int attempts = 0;
		while (true) {
			attempts++;
			if (attempts == 2) {
				mySqlDao.executeSQLStatement(UNLOCK_TABLES);
			}
			status = mySqlDao.executeSQLStatement(sqlDrop);
			if (status.isSuccess()) {
				break;
			} else if ((status.getSqlExceptionCode() == MysqlErrorNumbers.ER_LOCK_WAIT_TIMEOUT)
					|| (status.getSqlExceptionCode() == MysqlErrorNumbers.ER_LOCK_ABORTED)
					|| (status.getSqlExceptionCode() == MysqlErrorNumbers.ER_LOCK_OR_ACTIVE_TRANSACTION)
					|| (status.getSqlExceptionCode() == MysqlErrorNumbers.ER_LOCK_TABLE_FULL)) {
				// Logger.info("Lock wait timeout while dropping %s, waiting 60 seconds to try again");
				try {
					Thread.sleep(60000);
				} catch (InterruptedException e) {
				}
			} else {
				// Logger.error(String.format(
				// "Error dropping table %s, SQLStateCode = %s, SQLExceptionCode = %d"));
				throw new EPFDbException(
						String.format(
								"Error dropping table %s, SQLStateCode = %s, SQLExceptionCode = %d",
								status.getSqlStateCode(),
								status.getSqlExceptionCode()));
			}
		}
	}

	private void createTable(String tableName,
			LinkedHashMap<String, String> columnsAndTypes)
			throws EPFDbException {
		columnNames = "";
		String columnsToCreate = "";

		quotedColumnType = new boolean[columnsAndTypes.size()];
		int col = 0;

		Iterator<Entry<String, String>> entrySet = columnsAndTypes.entrySet()
				.iterator();
		while (entrySet.hasNext()) {
			Entry<String, String> colNType = entrySet.next();
			columnNames += "`" + colNType.getKey() + "`";
			columnsToCreate += "`" + colNType.getKey() + "` "
					+ translateColumnType(colNType.getValue());
			quotedColumnType[col++] = isQuotedColumnType(colNType.getValue());

			if (entrySet.hasNext()) {
				columnNames += ",";
				columnsToCreate += ", ";
			}
		}

		mySqlDao.executeSQLStatement(String.format(CREATE_TABLE_STMT,
				tableName, columnsToCreate));
	}

	/**
	 * Create a map of which fields to include in the import.
	 * <p/>
	 * For FULL and FLAT export types, import all fields.
	 * <p/>
	 * For INCREMENTAL exports, map the newly exported columns with the existing
	 * columns of the table to be updated. Any additional exported columns will
	 * be skipped during an INCREMENTAL update.
	 * 
	 * @param columnsAndTypes
	 * @param exportType
	 * @throws EPFDbException
	 */
	private void setupColumnMap(LinkedHashMap<String, String> columnsAndTypes,
			EPFExportType exportType) throws EPFDbException {

		columnMap = new HashMap<Integer, String>();
		quotedColumnType = new boolean[columnsAndTypes.size()];
		columnNames = "";
		int col = 0;
		
		// Full or Flat Export - Use All Columns
		if ((exportType == EPFExportType.FULL)
				|| (exportType == EPFExportType.FLAT)) {
			for (Entry<String,String> columnAndType : columnsAndTypes.entrySet()) {
				columnMap.put(Integer.valueOf(col), (String)columnAndType.getKey());
				quotedColumnType[col] = isQuotedColumnType(columnAndType.getValue());
				if (columnNames.length() > 0) {
					columnNames += ",";
				}
				columnNames += "`" + columnAndType.getKey() + "`";
				col++;
			}
		} else {
			// Incremental Update - use only the columns of the existing table
			List<String> currentColumns = mySqlDao.getTableColumns(tableName);

			if (currentColumns.size() == 0) {
				throw new EPFDbException(
						"Table does not exist and cannot be updated");
			}

			for (Entry<String,String> columnAndType : columnsAndTypes.entrySet()) {
				if (currentColumns.contains(columnAndType.getKey())) {
					columnMap.put(Integer.valueOf(col), columnAndType.getKey());
					quotedColumnType[col] = isQuotedColumnType(columnAndType.getValue());
				}
				if (columnNames.length() > 0) {
					columnNames += ",";
				}
				columnNames += "`" + columnAndType.getKey() + "`";
				col++;
			}
			
			if (columnMap.size() == 0) {
				throw new EPFDbException(
						"No import columns match destination table");
			}
		}
	}

	private boolean isQuotedColumnType(String columnType) {
		if (UNQUOTED_TYPES.contains(columnType)) {
			return false;
		}
		return true;
	}

	private String translateColumnType(String columnType) {
		if (TRANSLATION_MAP.containsKey(columnType)) {
			return TRANSLATION_MAP.get(columnType);
		}
		return columnType;
	}

	@Override
	public void setPrimaryKey(String tableName, String[] columnName)
			throws EPFDbException {
		primaryKey = Arrays.asList(columnName);

		String primaryKeyColumns = "";

		Iterator<String> entries = primaryKey.iterator();
		while (entries.hasNext()) {
			primaryKeyColumns += "`" + entries.next() + "`";
			if (entries.hasNext()) {
				primaryKeyColumns += ",";
			}
		}

		String sqlAlterTable = String.format(PRIMARY_KEY_STMT, tableName,
				primaryKeyColumns);

		executeSQLStatementWithRetry(sqlAlterTable);
	}

	@Override
	public void insertRow(String[] rowData) throws EPFDbException {
		if (insertBufferCount <= 0) {
			insertBuffer = "";
		}
		if (insertBuffer.length() > 0) {
			insertBuffer += ",";
		}
		insertBuffer += "(" + formatInsertRow(rowData) + ")";
		if (++insertBufferCount >= INSERT_BUFFER_SIZE) {
			flushInsertBuffer();
		}
	}

	public void flushInsertBuffer() throws EPFDbException {
		// commandString = ("REPLACE" if isIncremental else "INSERT")
		// exStrTemplate = """%s %s INTO %s %s VALUES %s"""
		// colNamesStr = "(%s)" % (", ".join(self.parser.columnNames))
		// colVals = unicode(", ".join(stringList), 'utf-8')
		// exStr = exStrTemplate % (commandString, ignoreString, tableName,
		// colNamesStr, colVals)
		if (insertBufferCount <= 0) {
			return;
		}

		String commandString = "INSERT";

		if (processMode == ProcessMode.APPEND) {
			commandString = "REPLACE";
		}

		executeSQLStatementWithRetry(String.format(INSERT_SQL_STMT,
				commandString, impTableName, columnNames, insertBuffer));
		insertBuffer = "";
		insertBufferCount = 0;
	}

	public String formatInsertRow(String[] rowData) {
		String row = "";
		for (int i = 0; i < rowData.length; i++) {
			// Include only columns mapped in columnMap
			if (columnMap.containsKey(Integer.valueOf(i))) {
				if (quotedColumnType[i]) {
					row += "'" + rowData[i] + "'";
				} else {
					row += rowData[i];
				}
				if (i + 1 < rowData.length) {
					row += ",";
				}
			}
		}
		return row;
	}

	public void executeSQLStatementWithRetry(String sqlStmt)
			throws EPFDbException {
		int attempts = 0;
		while (true) {
			attempts++;
			if (attempts == 2) {
				mySqlDao.executeSQLStatement(UNLOCK_TABLES);
			} else if (attempts > MAX_SQL_ATTEMPTS) {
				throw new EPFDbException(String.format(
						"Error applying primary key constraint: %s", sqlStmt));
			}
			SQLReturnStatus status = mySqlDao.executeSQLStatement(sqlStmt);
			if (status.isSuccess()) {
				break;
			} else if (status.getSqlExceptionCode() != MysqlErrorNumbers.ER_LOCK_WAIT_TIMEOUT) {
				throw new EPFDbException(
						String.format(
								"Error applying primary key constraint: SQLState %s, MySQLError %d",
								status.getSqlStateCode(),
								status.getSqlExceptionCode()));
			}
		}
	}

	@Override
	public void finalizeImport() throws EPFDbException {
		flushInsertBuffer();

		if (processMode == ProcessMode.MERGE_RENAME) {
			// Union Query with destination to the uncTableName
			mergeTables(tableName, impTableName, uncTableName);
			// Rename uncTableName tableName
			renameTableAndDrop(uncTableName, tableName);
			// Drop impTableName
			dropTable(impTableName);
		}

		if (processMode == ProcessMode.IMPORT_RENAME) {
			// Rename tmpTableName tableName
			renameTableAndDrop(impTableName, tableName);
		}
	}

	// CREATE TABLE unionTableName
	// IGNORE SELECT * FROM incTableName UNION ALL
	// SELECT * FROM tableName WHERE 0 =
	// (SELECT COUNT(*) FROM incTableName
	// WHERE tableName.export_date <= incTableName.export_date
	// AND tableName.primKey1 = incTableName,primKey1
	// AND tableName.primKey2 = incTableName.primKey2)
	private void mergeTables(String tableName, String incTableName,
			String unionTableName) throws EPFDbException {

		String mergeWhere = String.format(UNION_QUERY_WHERE, tableName,
				incTableName);

		Iterator<String> keyColumns = primaryKey.iterator();
		while (keyColumns.hasNext()) {
			String keyColumn = keyColumns.next();
			mergeWhere += " "
					+ String.format(UNION_QUERY_JOIN, tableName, keyColumn,
							incTableName, keyColumn);
		}

		String unionCreateTableSQL = String.format(UNION_QUERY_PT1,
				unionTableName, incTableName);
		unionCreateTableSQL += " "
				+ String.format(UNION_QUERY_PT2, tableName, incTableName,
						mergeWhere);

		executeSQLStatementWithRetry(unionCreateTableSQL);
	}

	private void renameTableAndDrop(String srcTableName, String destTableName)
			throws EPFDbException {
		String oldTableName = destTableName + "_old";
		// Drop the oldTableName just in case it was left from a previous run
		dropTable(oldTableName);
		String sql;
		SQLReturnStatus status = new SQLReturnStatus();
		status.setSuccess(true);
		if (isTableInDatabase(destTableName)) {
			sql = String.format(RENAME_TABLE_STMT, destTableName, oldTableName);
			status = mySqlDao.executeSQLStatement(sql);
		}
		sql = String.format(RENAME_TABLE_STMT, srcTableName, destTableName);
		status = mySqlDao.executeSQLStatement(sql);
		if (!status.isSuccess()) {
			// Logger.logInfo(String.format("Error renaming table srcTableName
			// to destTableName - reverting);
			sql = String.format(RENAME_TABLE_STMT, oldTableName, destTableName);
			mySqlDao.executeSQLStatement(sql);
		}
		// Drop the oldTableName
		dropTable(oldTableName);
	}

	@Override
	public boolean isTableInDatabase(String tableName) throws EPFDbException {
		return mySqlDao.isTableInDatabase(tableName);
	}

	@Override
	public int getTableColumnCount(String tableName) throws EPFDbException {
		return mySqlDao.getTableColumns(tableName).size();
	}
}

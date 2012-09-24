package com.spazzmania.epf.ingester;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
public class EPFDbWriterMySQL extends EPFDbWriter {

	public static String DROP_TABLE_STMT = "DROP TABLE IF EXISTS %s";
	public static String CREATE_TABLE_STMT = "CREATE TABLE %s (%s)";
	public static String RENAME_TABLE_STMT = "ALTER TABLE %s RENAME %s";
	public static String PRIMARY_KEY_STMT = "ALTER TABLE %s ADD CONSTRAINT PRIMARY KEY (%s)";
	public static String INSERT_SQL_STMT = "%s INTO %s (%s) VALUES %s";
	public static String COLUMN_NAMES_SQL = "SELECT * FROM `%s` LIMIT 1";
	public static String TABLE_EXISTS_SQL = "SHOW TABLES";
	public static String UNLOCK_TABLES = "UNLOCK TABLES";

	public static int EXECUTE_SQL_STATEMENT_RETRIES = 30;
	public static long MERGE_THRESHOLD = 500000;
	public static long INSERT_BUFFER_SIZE = 200;
	public static int MAX_SQL_RETRIES = 3;

	public EPFDbWriterMySQL() {
		super();
	}

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
	private Map<String, Integer> columnMap;

	private int insertBufferCount = 0;
	String insertBuffer;

	private enum ProcessMode {
		IMPORT_RENAME, APPEND, MERGE_RENAME
	};

	private ProcessMode processMode;

	@Override
	public void initImport(EPFExportType exportType, String tableName,
			LinkedHashMap<String, String> columnsAndTypes, long numberOfRows)
			throws EPFDbException {

		this.tableName = getTablePrefix() + tableName;
		this.impTableName = this.tableName + "_tmp";
		this.uncTableName = null;
		dropTable(impTableName);
		if (exportType == EPFExportType.FULL) {
			processMode = ProcessMode.IMPORT_RENAME;
		} else {
			processMode = ProcessMode.APPEND;
			if (numberOfRows >= MERGE_THRESHOLD) {
				processMode = ProcessMode.MERGE_RENAME;
				uncTableName = this.tableName + "_unc";
				dropTable(uncTableName);
			}
		}
		setupColumnMap(columnsAndTypes);
		createTable(impTableName, columnsAndTypes);
		insertBufferCount = 0;
	}

	private void dropTable(String tableName) throws EPFDbException {
		String sqlDrop = String.format(DROP_TABLE_STMT, tableName);
		SQLReturnStatus status;
		int attempts = 0;
		while (true) {
			attempts++;
			if (attempts == 2) {
				executeSQLStatement(UNLOCK_TABLES);
			}
			status = executeSQLStatement(sqlDrop);
			if (status.success) {
				break;
			} else if (status.sqlExceptionCode == MySQLErrorCodes.ER_LOCK_WAIT_TIMEOUT) {
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
								status.sqlStateCode, status.sqlExceptionCode));
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

		executeSQLStatement(String.format(CREATE_TABLE_STMT, tableName,
				columnsToCreate));
	}

	private void setupColumnMap(LinkedHashMap<String, String> columnsAndTypes)
			throws EPFDbException {
		Map<String, Integer> currentColumnMap = getCurrentTableColumns(tableName);

		if (currentColumnMap.size() == 0) {
			throw new EPFDbException(
					"Table does not exist and cannot be updated");
		}

		columnMap = new HashMap<String, Integer>();

		for (String columnName : columnsAndTypes.keySet()) {
			if (currentColumnMap.containsKey(columnName)) {
				columnMap.put(columnName, currentColumnMap.get(columnName));
			}
		}

		if (columnMap.size() == 0) {
			throw new EPFDbException(
					"No import columns match destination table");
		}
	}

	/**
	 * Retrieve a Map of Column Names and ordinal positions for the current
	 * table. If the table doesn't exist, return an empty Map.
	 * 
	 * @param tableName
	 * @return
	 * @throws EPFDbException
	 *             - if it cannot get a db connection
	 */
	private Map<String, Integer> getCurrentTableColumns(String tableName)
			throws EPFDbException {
		Connection connection = null;

		String sqlStmt = String.format(COLUMN_NAMES_SQL, tableName);
		Map<String, Integer> tableColumns = new HashMap<String, Integer>();
		Statement st;

		try {
			connection = getConnection();
			st = connection.createStatement();
			ResultSet resultSet = st.executeQuery(sqlStmt);
			// ResultSetMetaData metaData =
			// st.executeQuery(sqlStmt).getMetaData();
			ResultSetMetaData metaData = resultSet.getMetaData();
			for (int i = 0; i < metaData.getColumnCount(); i++) {
				tableColumns.put(metaData.getColumnName(i), Integer.valueOf(i));
			}
		} catch (Exception e) {
			e = e;
			// Ignore all errors & exceptions...
			// If the table does not exist because it hasn't
			// previously been imported, return an empty tableColumns list.
		} catch (Error e) {
			e = e;
		}

		releaseConnection(connection);

		return tableColumns;
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
		String primaryKeyColumns = "";
		for (int i = 0; i < columnName.length; i++) {
			primaryKeyColumns += "`" + columnName[i] + "`";
			if (i + 1 < columnName.length) {
				primaryKeyColumns += ",";
			}
		}
		String sqlAlterTable = String.format(PRIMARY_KEY_STMT, tableName,
				primaryKeyColumns);
		executeSQLStatement(sqlAlterTable);
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
			insertBufferCount = 0;
		}
	}

	public void flushInsertBuffer() throws EPFDbException {
		// commandString = ("REPLACE" if isIncremental else "INSERT")
		// exStrTemplate = """%s %s INTO %s %s VALUES %s"""
		// colNamesStr = "(%s)" % (", ".join(self.parser.columnNames))
		// colVals = unicode(", ".join(stringList), 'utf-8')
		// exStr = exStrTemplate % (commandString, ignoreString, tableName,
		// colNamesStr, colVals)
		String commandString = "INSERT";

		if (processMode == ProcessMode.APPEND) {
			commandString = "REPLACE";
		}

		executeSQLStatement(String.format(INSERT_SQL_STMT, commandString,
				impTableName, columnNames, insertBuffer));
	}

	public String formatInsertRow(String[] rowData) {
		String row = "";
		for (int i = 0; i < rowData.length; i++) {
			if (quotedColumnType[i]) {
				row += "'" + rowData[i] + "'";
			} else {
				row += rowData[i];
			}
			if (i + 1 < rowData.length) {
				row += ",";
			}
		}
		return row;
	}

	private class SQLReturnStatus {
		public String sqlStateCode;
		public Integer sqlExceptionCode;
		public boolean success;
	}

	private SQLReturnStatus executeSQLStatement(String sqlStmt)
			throws EPFDbException {
		SQLReturnStatus sqlStatus = new SQLReturnStatus();

		Connection connection = null;

		Statement st;
		try {
			connection = getConnection();
			st = connection.createStatement();
			st.execute(sqlStmt);
			sqlStatus.success = true;
		} catch (SQLException e1) {
			sqlStatus.success = false;
			sqlStatus.sqlStateCode = SQLUtil.getSQLStateCode(e1.getSQLState());
			sqlStatus.sqlExceptionCode = e1.getErrorCode();
		}
		releaseConnection(connection);
		return sqlStatus;
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
			renameTableAndDrop(uncTableName, tableName);
		}
	}

	private void mergeTables(String srcTableName1, String srcTableName2,
			String destTableName) {

	}

	private void renameTableAndDrop(String srcTableName, String destTableName)
			throws EPFDbException {
		String oldTableName = destTableName + "_old";
		// Drop the oldTableName just in case it was left from a previous run
		dropTable(oldTableName);
		String sql;
		SQLReturnStatus status = new SQLReturnStatus();
		status.success = true;
		if (isTableInDatabase(destTableName)) {
			sql = String.format(RENAME_TABLE_STMT, destTableName, oldTableName);
			status = executeSQLStatement(sql);
		}
		sql = String.format(RENAME_TABLE_STMT, srcTableName, destTableName);
		status = executeSQLStatement(sql);
		if (!status.success) {
			// Logger.logInfo(String.format("Error renaming table srcTableName
			// to destTableName - reverting);
			sql = String.format(RENAME_TABLE_STMT, oldTableName, destTableName);
			executeSQLStatement(sql);
		}
		// Drop the oldTableName
		dropTable(oldTableName);
	}

	@Override
	public boolean isTableInDatabase(String tableName) throws EPFDbException {
		DatabaseMetaData dbm;
		Connection connection = null;
		try {
			connection = getConnection();
			dbm = connection.getMetaData();
			String types[] = { "TABLE" };
			ResultSet tables = dbm.getTables(null, null, tableName, types);
			tables.beforeFirst();
			if (tables.next()) {
				return true;
			}
		} catch (SQLException e) {
			// IGNORE - an error will occur when the table doesn't exist
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
				// Ignore - this may attempting to close a failed connection
			}
		}

		return false;
	}

	@Override
	public int getTableColumnCount(String tableName) throws EPFDbException {
		DatabaseMetaData dbm;
		Connection connection = null;
		try {
			connection = getConnection();
			dbm = connection.getMetaData();
			// Get the list of table columns as a SQL Result Set
			ResultSet columns = dbm.getColumns(null, null, tableName, null);
			columns.last(); // Move to the last row of the result set
			return columns.getRow(); // return the row number as the column
										// count
		} catch (SQLException e) {
			// IGNORE - an error will occur when the table doesn't exist
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
				// Ignore - this may attempting to close a failed connection
			}
		}

		return 0;
	}
}

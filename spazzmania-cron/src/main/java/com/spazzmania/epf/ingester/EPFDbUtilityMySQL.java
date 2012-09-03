package com.spazzmania.epf.ingester;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * EPFDbUtilityMySQL - a MySQL Implementation of the EPFDbUtility interface.
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
public class EPFDbUtilityMySQL implements EPFDbUtility {

	private Connection connection;

	public static long MERGE_THRESHOLD = 500000;
	public static long INSERT_BUFFER_SIZE = 200;
	public static Map<String, String> TRANSLATION_MAP = Collections
			.unmodifiableMap(new HashMap<String, String>() {
				private static final long serialVersionUID = 1L;
				{
					put("CLOB", "LONGTEXT");
				}
			});

	private String tableName;
	private String impTableName;
	private String uncTableName;
	private int insertBuffer = 0;
	LinkedHashMap<String, String> columnsAndTypes;

	private enum ProcessMode {
		IMPORT_RENAME, APPEND, MERGE_RENAME
	};

	private ProcessMode processMode;

	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	@Override
	public void initImport(EPFImportType importType, String tableName,
			long numberOfRows) {
		this.tableName = tableName;
		this.impTableName = null;
		this.uncTableName = null;
		if (importType == EPFImportType.FULL) {
			processMode = ProcessMode.IMPORT_RENAME;
			dropTable(tableName);
			impTableName = this.tableName;
		} else {
			processMode = ProcessMode.APPEND;
			impTableName = this.tableName + "_tmp";
			dropTable(impTableName);
			if (numberOfRows >= MERGE_THRESHOLD) {
				processMode = ProcessMode.MERGE_RENAME;
				uncTableName = this.tableName + "_unc";
				dropTable(uncTableName);
			}
		}
		insertBuffer = 0;
	}

	public static int DROP_TABLE_RETRIES = 30;

	private void dropTable(String tableName) {
		String sqlDrop = String.format(DROP_TABLE_STMT, tableName);
		boolean completed = false;

		int retries = 0;
		while (!completed) {
			retries++;
			Statement st;
			try {
				st = connection.createStatement();
				st.execute(sqlDrop);
			} catch (SQLException e1) {
				e1.printStackTrace();
				if (SQLUtil.getSQLStateCode(e1.getSQLState()) == SQLUtil.INTEGRITY_CONSTRAINT_VIOLATION) {
					// Probably a primary key error - report the error and
					// return
					// Logger.logError("Error %d: %s",e1.getErrorCode(),e1.getMessage);
					completed = true;
					break;
				} else if ((e1.getErrorCode() != MySQLErrorCodes.ER_LOCK_WAIT_TIMEOUT)
						|| (retries >= DROP_TABLE_RETRIES)) {
					// Raise the error
					throw new RuntimeException(e1.getMessage());
				}
			}

			try {
				Thread.sleep(60);
			} catch (InterruptedException e1) {
				// Ignore and interrupted sleep error
			}
		}
	}

	public static String DROP_TABLE_STMT = "DROP TABLE IF EXISTS %s";
	public static String CREATE_TABLE_STMT = "CREATE TABLE %s (%s)";

	@Override
	public void createTable(LinkedHashMap<String, String> columnsAndTypes) {
		this.columnsAndTypes = columnsAndTypes;

		String columnNames = "";
		String columnTypes = "";

		Iterator<Entry<String, String>> entrySet = columnsAndTypes.entrySet()
				.iterator();
		while (entrySet.hasNext()) {
			Entry<String, String> colNType = entrySet.next();
			columnNames += "`" + colNType.getKey() + "`";
			columnTypes += translateColumnType(colNType.getValue());

			if (entrySet.hasNext()) {
				columnNames += ",";
				columnTypes += ",";
			}
		}

		try {
			Statement st = connection.createStatement();
			st.execute(String.format(DROP_TABLE_STMT, impTableName));
			st.execute(String.format(CREATE_TABLE_STMT, columnNames,
					columnTypes));
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private String translateColumnType(String columnType) {
		if (TRANSLATION_MAP.containsKey(columnType)) {
			return TRANSLATION_MAP.get(columnType);
		}
		return columnType;
	}

	@Override
	public void setPrimaryKey(String[] columnName) {
		// TODO Auto-generated method stub

	}

	@Override
	public void insertRow(String[] rowData) {
		// TODO Auto-generated method stub

	}

	private int executeSQLStatement(String sqlStmt,
			List<Integer> ignoreSQLErrors) {
		boolean completed = false;
		int sqlError = 0;
		while (completed) {
			Statement st = null;
			try {
				st = connection.createStatement();
				st.execute(sqlStmt);
				completed = true;
			} catch (SQLException e) {
				sqlError = e.getErrorCode();
				// First - check if the code returned is something to ignore
				if (ignoreSQLErrors.contains(sqlError)) {
					// Ignore the error and return
					completed = true;
					sqlError = 0;
				}
			}
		}
		return sqlError;
	}

	@Override
	public void finalizeImport() {
		// TODO Auto-generated method stub

	}

}

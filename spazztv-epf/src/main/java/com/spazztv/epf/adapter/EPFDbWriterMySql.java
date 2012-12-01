package com.spazztv.epf.adapter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import com.mysql.jdbc.MysqlErrorNumbers;
import com.spazztv.epf.EPFExportType;
import com.spazztv.epf.dao.EPFDbException;
import com.spazztv.epf.dao.EPFDbWriter;

/**
 * A MySQL Implementation of the EPFDbWriter class.
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

	public static String UNLOCK_TABLES = "UNLOCK TABLES";

	public static int EXECUTE_SQL_STATEMENT_RETRIES = 30;
	public static long MERGE_THRESHOLD = 500000;
	public static int INSERT_BUFFER_SIZE = 200;
	public static int MAX_SQL_ATTEMPTS = 3;

	private EPFDbWriterMySqlStmt mySqlStmt;
	private EPFDbWriterMySqlDao mySqlDao;

	private String tableName;
	private String impTableName;
	private String uncTableName;

	private LinkedHashMap<String, String> columnsAndTypes;
	private List<String> primaryKey;

	private int insertBufferCount = 0;
	private List<List<String>> insertBuffer;
	
	private long totalRowsInserted = 0;

	private enum ProcessMode {
		IMPORT_RENAME, APPEND, MERGE_RENAME
	};

	private ProcessMode processMode;

	public EPFDbWriterMySql() {
		super();
		mySqlDao = new EPFDbWriterMySqlDao(this);
		mySqlStmt = new EPFDbWriterMySqlStmt();
	}

	public void setMySqlDao(EPFDbWriterMySqlDao mySqlDao) {
		this.mySqlDao = mySqlDao;
	}

	public void setMySqlStmt(EPFDbWriterMySqlStmt mySqlStmt) {
		this.mySqlStmt = mySqlStmt;
	}

	@Override
	public void initImport(EPFExportType exportType, String tableName,
			LinkedHashMap<String, String> columnsAndTypes, List<String> primaryKey, long numberOfRows)
			throws EPFDbException {

		this.tableName = getTablePrefix() + tableName;
		// Default method is to create a temporary table and append data
		this.impTableName = this.tableName + "_tmp";
		this.uncTableName = null;
		this.primaryKey = primaryKey;

		setupColumnMap(columnsAndTypes, exportType);

		if (exportType == EPFExportType.FULL) {
			processMode = ProcessMode.IMPORT_RENAME;
		} else {
			if (!mySqlDao.isTableInDatabase(this.tableName)) {
				throw new EPFDbException("Table not found - cannot append data");
			}
			if (numberOfRows < MERGE_THRESHOLD) {
				// Append directly to the existing table
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
		String sqlDrop = mySqlStmt.dropTableStmt(tableName);
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
		String createTableStmt = mySqlStmt.createTableStmt(tableName,
				columnsAndTypes);
		mySqlDao.executeSQLStatement(createTableStmt);
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

		List<String> currentColumns = null;
		if ((exportType == EPFExportType.INCREMENTAL)
				|| (exportType == EPFExportType.FLAT)) {
			currentColumns = mySqlDao.getTableColumns(tableName);
		}
		this.columnsAndTypes = mySqlStmt.setupColumnAndTypesMap(
				columnsAndTypes, currentColumns);
	}

	private void applyPrimaryKey(String tableName) throws EPFDbException {
		if (primaryKey != null) {
			String sqlAlterTable = mySqlStmt.setPrimaryKeyStmt(tableName,
					primaryKey);
	
			executeSQLStatementWithRetry(sqlAlterTable);
		}
	}

	@Override
	public void insertRow(String[] rowData) throws EPFDbException {
		if (insertBufferCount <= 0) {
			insertBuffer = new ArrayList<List<String>>();
		}
		insertBuffer.add(Arrays.asList(rowData));
		insertBufferCount++;
		if (insertBufferCount >= INSERT_BUFFER_SIZE) {
			flushInsertBuffer();
		}
	}

	private void flushInsertBuffer() throws EPFDbException {
		if (insertBufferCount <= 0) {
			return;
		}

		String insertCommand = "INSERT";

		if (processMode == ProcessMode.APPEND) {
			insertCommand = "REPLACE";
		}

		String insertStmt = mySqlStmt.insertRowStmt(impTableName,
				columnsAndTypes, insertBuffer, insertCommand);
		executeSQLStatementWithRetry(insertStmt);

		totalRowsInserted += insertBufferCount;
		insertBufferCount = 0;
	}

	private void executeSQLStatementWithRetry(String sqlStmt)
			throws EPFDbException {
		int attempts = 0;
		while (true) {
			attempts++;
			if (attempts == 2) {
				mySqlDao.executeSQLStatement(UNLOCK_TABLES);
			} else if (attempts > MAX_SQL_ATTEMPTS) {
				throw new EPFDbException(String.format(
						"Error executing SQL Statement: %s", sqlStmt));
			}
			SQLReturnStatus status = mySqlDao.executeSQLStatement(sqlStmt);
			if (status.isSuccess()) {
				break;
			} else if (status.getSqlExceptionCode() != MysqlErrorNumbers.ER_LOCK_WAIT_TIMEOUT) {
				throw new EPFDbException(
						String.format(
								"Error executing SQL Statement: \"%s...\", sqlStmt.substring(40), SQLState %s, MySQLError %d",
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
			// Apply primary key
			applyPrimaryKey(uncTableName);
			// Rename uncTableName tableName
			renameTableAndDrop(uncTableName, tableName);
			// Drop impTableName
			dropTable(impTableName);
		}

		if (processMode == ProcessMode.IMPORT_RENAME) {
			// Apply primary key
			applyPrimaryKey(impTableName);
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

		String mergeTableSql = mySqlStmt.mergeTableStmt(tableName,
				incTableName, unionTableName, primaryKey);

		executeSQLStatementWithRetry(mergeTableSql);
	}

	private void renameTableAndDrop(String srcTableName, String destTableName)
			throws EPFDbException {
		String oldTableName = destTableName + "_old";

		// Drop the oldTableName just in case it was left from a previous run
		dropTable(oldTableName);

		SQLReturnStatus status;

		// If the destination table exists, temporarily rename it
		if (isTableInDatabase(destTableName)) {
			status = renameTable(destTableName, oldTableName);
		} else {
			oldTableName = null;
		}

		// Execute the main table rename
		status = renameTable(srcTableName, destTableName);

		// If not successful, put back the old table and return
		if (!status.isSuccess() && oldTableName != null) {
			// Drop the oldTableName
			renameTable(oldTableName, destTableName);
			return;
		}

		// Drop the old table if it exists
		if (oldTableName != null) {
			dropTable(oldTableName);
		}
	}

	private SQLReturnStatus renameTable(String oldTableName, String newTableName)
			throws EPFDbException {
		String sql = mySqlStmt.renameTableStmt(oldTableName, newTableName);
		return mySqlDao.executeSQLStatement(sql);
	}

	@Override
	public boolean isTableInDatabase(String tableName) throws EPFDbException {
		return mySqlDao.isTableInDatabase(tableName);
	}

	@Override
	public int getTableColumnCount(String tableName) throws EPFDbException {
		return mySqlDao.getTableColumns(tableName).size();
	}

	@Override
	public Long getTotalRowsInserted() {
		return totalRowsInserted;
	}
}

package com.spazztv.epf.adapter;

import java.util.LinkedHashMap;
import java.util.List;

import com.mysql.jdbc.MysqlErrorNumbers;
import com.spazztv.epf.EPFExportType;
import com.spazztv.epf.dao.EPFDbException;
import com.spazztv.epf.dao.EPFDbWriter;

/**
 * An Oracle Implementation of the EPFDbWriter class.
 * <p>
 * This implementation is similar to the original EPF Python scripts but creates
 * tables in an Oracle DB using Oracle Column Types.
 * 
 * <p>
 * This has three modes of import with three very different modes of operation:
 * <ul>
 * <li/><b>Full</b> import of the file replacing any previous table and data of
 * the same name. Data is imported to a temporary table first and on successful
 * completion of the import, the original table is dropped and the temporary
 * table is renamed.
 * <li/><b>Append</b> import is an <i>Incremental</i> importing of records
 * appending/merging them directly to the existing table. This method is done in
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
public class EPFDbWriterOracle extends EPFDbWriter {

	public static String UNLOCK_TABLES = "UNLOCK TABLES";

	public static int EXECUTE_SQL_STATEMENT_RETRIES = 30;
	public static long MERGE_THRESHOLD = 500000;
	public static int INSERT_BUFFER_SIZE = 200;
	public static int MAX_SQL_ATTEMPTS = 3;

	private EPFDbWriterOracleStmt sqlStmt;
	private EPFDbWriterOracleDao sqlDao;

	private String tableName;
	private String impTableName;
	private String uncTableName;
	
	private long totalRecordsInserted = 0;

	private LinkedHashMap<String, String> columnsAndTypes;
	private List<String> primaryKey;

	private enum ProcessMode {
		IMPORT_RENAME, IMPORT_MERGE, MERGE_RENAME
	};

	private ProcessMode processMode;

	public EPFDbWriterOracle() {
		super();
		sqlDao = new EPFDbWriterOracleDao(this);
		sqlStmt = new EPFDbWriterOracleStmt();
	}

	public void setMySqlDao(EPFDbWriterOracleDao mySqlDao) {
		this.sqlDao = mySqlDao;
	}

	public void setMySqlStmt(EPFDbWriterOracleStmt mySqlStmt) {
		this.sqlStmt = mySqlStmt;
	}

	@Override
	public void initImport(EPFExportType exportType, String tableName,
			LinkedHashMap<String, String> columnsAndTypes,
			List<String> primaryKey, long numberOfRows) throws EPFDbException {

		this.tableName = getTablePrefix() + tableName;
		// Default method is to create a temporary table and append data
		this.impTableName = this.tableName + "_tmp";
		this.uncTableName = null;
		this.primaryKey = primaryKey;

		setupColumnMap(columnsAndTypes, exportType);

		if (exportType == EPFExportType.FULL) {
			processMode = ProcessMode.IMPORT_RENAME;
		} else {
			if (!sqlDao.isTableInDatabase(this.tableName)) {
				throw new EPFDbException("Table not found - cannot append data");
			}
			if (numberOfRows < MERGE_THRESHOLD) {
				// Append directly to the existing table
				processMode = ProcessMode.IMPORT_MERGE;
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
	}

	private void dropTable(String tableName) throws EPFDbException {
		String sqlDrop = sqlStmt.dropTableStmt(tableName);
		SQLReturnStatus status;
		int attempts = 0;
		while (true) {
			attempts++;
			if (attempts == 2) {
				sqlDao.executeSQLStatement(UNLOCK_TABLES);
			}
			status = sqlDao.executeSQLStatement(sqlDrop);
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
		String createTableStmt = sqlStmt.createTableStmt(tableName,
				columnsAndTypes);
		sqlDao.executeSQLStatement(createTableStmt);
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
			currentColumns = sqlDao.getTableColumns(tableName);
		}
		this.columnsAndTypes = sqlStmt.setupColumnAndTypesMap(columnsAndTypes,
				currentColumns);
	}

	private void applyPrimaryKey(String tableName) throws EPFDbException {

		String sqlAlterTable = sqlStmt.setPrimaryKeyStmt(tableName, primaryKey);

		executeSQLStatement(sqlAlterTable);
	}

	@Override
	public void insertRow(List<String> rowData) throws EPFDbException {
		String insertStmt = sqlStmt.insertRowStmt(impTableName,
				columnsAndTypes, rowData);
		executeSQLStatement(insertStmt);
		totalRecordsInserted++;
	}

	private void executeSQLStatement(String sqlStmt) throws EPFDbException {
		SQLReturnStatus status = sqlDao.executeSQLStatement(sqlStmt);
		if (!status.isSuccess()) {
			throw new EPFDbException(
					String.format(
							"Error executing SQL Statement: \"%s...\", sqlStmt.substring(40), SQLState %s, MySQLError %d",
							status.getSqlStateCode(),
							status.getSqlExceptionCode()));
		}
	}

	@Override
	public void finalizeImport() throws EPFDbException {
		if (processMode == ProcessMode.MERGE_RENAME) {
			// Union Query with destination to the uncTableName
			mergeTables(tableName, impTableName, uncTableName);
			// Apply primary key
			applyPrimaryKey(uncTableName);
			// Rename uncTableName tableName
			renameTableAndDrop(uncTableName, tableName);
			// Drop impTableName
			dropTable(impTableName);
		} else if (processMode == ProcessMode.IMPORT_MERGE) {
			mergeUpdateTable(tableName, impTableName);
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

		String mergeTableSql = sqlStmt.mergeTableStmt(tableName, incTableName,
				unionTableName, primaryKey);

		executeSQLStatement(mergeTableSql);
	}

	private void mergeUpdateTable(String tableName, String impTableName)
			throws EPFDbException {

		String mergeTableSql = sqlStmt.mergeUpdateTableStmt(tableName,
				impTableName, columnsAndTypes, primaryKey);

		executeSQLStatement(mergeTableSql);
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
		String sql = sqlStmt.renameTableStmt(oldTableName, newTableName);
		return sqlDao.executeSQLStatement(sql);
	}

	@Override
	public boolean isTableInDatabase(String tableName) throws EPFDbException {
		return sqlDao.isTableInDatabase(tableName);
	}

	@Override
	public int getTableColumnCount(String tableName) throws EPFDbException {
		return sqlDao.getTableColumns(tableName).size();
	}

	@Override
	public Long getTotalRowsInserted() {
		return totalRecordsInserted;
	}
	
	
}

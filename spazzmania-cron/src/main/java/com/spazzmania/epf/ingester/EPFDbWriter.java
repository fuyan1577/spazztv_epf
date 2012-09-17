/**
 * 
 */
package com.spazzmania.epf.ingester;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;

import com.jolbox.bonecp.BoneCP;

/**
 * Abstract class for creating, dropping, merging and updating an EPF Database.
 * <p/>
 * The methods defined here are based on the original Apple EPF Python scripts
 * intended for a MySQL MyISAM database. To understand the methods defined here,
 * a description of the original Python scripts is necessary. The following
 * logic was employed:
 * <p>
 * <ul>
 * <li/><i>Full Ingest</i>: All previous tables were dropped and new ones are
 * created
 * <li/><i>Incremental Ingest and Rows To Import < 500,000</i>: Append data to
 * destination table
 * <li/><i>Incremental Ingest and Rows To Import >= 500,000</i>
 * <ol>
 * <li/>Import data to temporary table
 * <li/>Merge original and temp tables into a new union table
 * <li/>Drop the original table and the temp table
 * <li/>Rename the union table to the original table name
 * </ol>
 * </ul>
 * 
 * <p>
 * Because the original script followed this logic, the importing logic requires
 * the following abstract methods to be implemented:
 * <p>
 * <ul>
 * <li/>public void initImport(EPFImportType importType, String tableName, long
 * numberOfRows);
 * <li/>public void setPrimaryKey(String columnName);
 * <li/>public void insertRow(String[] rowData);
 * <li/>public void finalizeImport();
 * </ul>
 * 
 * <p/>
 * <i>Note:</i> A Database Connection Pool is used to optimize the processing of
 * multiple threads. The implementing classes should call the method
 * <code>getConnection()</code> and <code>releaseConnection()</code> at the
 * beginning and end of each method that executes SQL statements.
 * 
 * @author Thomas Billingsley
 */
public abstract class EPFDbWriter {

	private BoneCP connectionPool;
	private String tablePrefix;

	public EPFDbWriter(String tablePrefix, BoneCP connectionPool) {
		this.tablePrefix = tablePrefix;
		this.connectionPool = connectionPool;
	}

	/**
	 * Get the tablePrefix to prepend to the EPF Table Names.
	 * 
	 * @return the tablePrefix
	 */
	public String tablePrefix() {
		return tablePrefix;
	}

	/**
	 * Initialize the table to be imported based on the importType and number of
	 * records.
	 * 
	 * @param importType
	 *            Enum <i>FULL</i> or <i>INCREMENTAL</i>
	 * @param tableName
	 *            Name of the table to be created from the import
	 * @param columnsAndTypes
	 *            Column Names and Data Types for the table
	 * @param numberOfRows
	 *            Number of rows in the import file
	 */
	public abstract void initImport(EPFExportType exportType, String tableName,
			LinkedHashMap<String, String> columnsAndTypes, long numberOfRows)
			throws EPFImporterException;

	/**
	 * Set the column that is the Primary Key. Single column primary keys are
	 * assumed.
	 * 
	 * @param columnName
	 */
	public abstract void setPrimaryKey(String[] columnName)
			throws EPFImporterException;

	/**
	 * Insert a row of data from the input String[] array. The field types are
	 * appropriately quoted and formatted per the destination database.
	 * 
	 * <p>
	 * The implementation of this method may queue up and perform batch inserts
	 * for optimizations. The finalizeImport() method is where any remaining
	 * queued rows are inserted.
	 * 
	 * @param rowData
	 *            - a String[] array of column data comprising one row
	 */
	public abstract void insertRow(String[] rowData)
			throws EPFImporterException;

	/**
	 * Finalize any table insert optimization. The original EPF Python script
	 * logic would insert any remaining queued rows and possibly merge, drop and
	 * rename tables.
	 */
	public abstract void finalizeImport() throws EPFImporterException;

	/**
	 * Sets the connection pool connector.
	 * 
	 * <p/>
	 * Used by the instantiating class to set the connection pool.
	 * 
	 * @param connector
	 *            the connector to set
	 */
	public final void setConnectionPool(BoneCP connector) {
		this.connectionPool = connector;
	}

	/**
	 * Provide access to the connection pool connector for the implementing
	 * classes.
	 * 
	 * @return
	 * @throws SQLException
	 */
	public final Connection getConnection() throws EPFImporterException {
		Connection conn;
		try {
			conn = connectionPool.getConnection();
		} catch (SQLException e) {
			throw new EPFImporterException(e.getMessage());
		}
		return conn;
	}

	/**
	 * Release the connection to the Connection Pool.
	 * 
	 * @param connection
	 */
	public final void releaseConnection(Connection connection)
			throws EPFImporterException {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				throw new EPFImporterException(e.getMessage());
			}
		}
	}

	/**
	 * Returns whether or not the table exists in the database.
	 * <p/>
	 * Used for <i>INCREMENTAL</i> imports when determining whether to continue
	 * with the import.
	 * 
	 * @param tableName
	 * @return true if the table exists
	 */
	public abstract boolean isTableInDatabase(String tableName)
			throws EPFImporterException;

	/**
	 * Returns the number of columns for <i>tableName</i>.
	 * <p/>
	 * Used for <i>INCREMENTAL</i> imports when determining whether to continue
	 * with the imports.
	 * 
	 * @param tableName
	 * @return the number of columns
	 */
	public abstract int getTableColumnCount(String tableName) throws EPFImporterException;
}

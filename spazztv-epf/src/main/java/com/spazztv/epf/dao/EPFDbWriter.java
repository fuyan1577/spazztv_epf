/**
 * 
 */
package com.spazztv.epf.dao;

import java.sql.Connection;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;

import com.spazztv.epf.EPFExportType;

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

	private EPFDbConnector connector;
	private String tablePrefix = "";
	private boolean skipKeyViolators;
	
	private EPFExportType exportType;
	private String tableName;
	private LinkedHashMap<String,String>columnsAndTypes;
	private List<String> primaryKey;
	private long numberOfRows;
	
	public static Hashtable<String,Boolean> dateFieldTypes;

	public EPFDbWriter() {
		dateFieldTypes = new Hashtable<String,Boolean>();
		dateFieldTypes.put("DATE", true);
		dateFieldTypes.put("DATETIME", true);
		dateFieldTypes.put("TIME", true);
		dateFieldTypes.put("TIMESTAMP", true);
	}

	/**
	 * Get the tablePrefix to prepend to the EPF Table Names.
	 * 
	 * @return the tablePrefix
	 */
	public final String getTablePrefix() {
		return tablePrefix;
	}

	/**
	 * Set the prefix to prepend to the import table names in the destination
	 * database.
	 * 
	 * @param tablePrefix
	 */
	public final void setTablePrefix(String tablePrefix) {
		this.tablePrefix = tablePrefix;
	}

	public final boolean isSkipKeyViolators() {
		return skipKeyViolators;
	}

	public final void setSkipKeyViolators(boolean skipKeyViolators) {
		this.skipKeyViolators = skipKeyViolators;
	}

	/**
	 * Sets the connection pool connector.
	 * 
	 * <p/>
	 * Used by the instantiating class to set the connection pool.
	 * 
	 * @param connector
	 *            the connector to set
	 */
	public final void setConnector(EPFDbConnector connector) {
		this.connector = connector;
	}

	/**
	 * Get a connection from the connection pool.
	 * 
	 * @return
	 */
	public final Connection getConnection() throws EPFDbException {
		return connector.getConnection();
	}

	/**
	 * Release the connection to the Connection Pool.
	 * 
	 * @param connection
	 */
	public final void releaseConnection(Connection connection)
			throws EPFDbException {
		if (connection != null) {
			connector.releaseConnection(connection);
		}
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
	public void initImport(EPFExportType exportType, String tableName,
			LinkedHashMap<String, String> columnsAndTypes, List<String> primaryKey, long numberOfRows) 
			throws EPFDbException {
		this.exportType = exportType;
		this.tableName = tableName;
		this.columnsAndTypes = columnsAndTypes;
		this.primaryKey = primaryKey;
		this.numberOfRows = numberOfRows;
	}
	
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
	public abstract void insertRow(List<String> rowData) throws EPFDbException;

	/**
	 * Finalize any table insert optimization. The original EPF Python script
	 * logic would insert any remaining queued rows and possibly merge, drop and
	 * rename tables.
	 */
	public abstract void finalizeImport() throws EPFDbException;

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
			throws EPFDbException;

	/**
	 * Returns the number of columns for <i>tableName</i>.
	 * <p/>
	 * Used for <i>INCREMENTAL</i> imports when determining whether to continue
	 * with the imports.
	 * 
	 * @param tableName
	 * @return the number of columns
	 */
	public abstract int getTableColumnCount(String tableName)
			throws EPFDbException;
	
	/**
	 * Returns the number of rows inserted
	 * @return rows inserted
	 */
	public abstract Long getTotalRowsInserted();

	/**
	 * @return the exporType
	 */
	public EPFExportType getExportType() {
		return exportType;
	}

	/**
	 * @param exporType the exporType to set
	 */
	public void setExportType(EPFExportType exportType) {
		this.exportType = exportType;
	}

	/**
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @param tableName the tableName to set
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * @return the columnsAndTypes
	 */
	public LinkedHashMap<String, String> getColumnsAndTypes() {
		return columnsAndTypes;
	}

	/**
	 * @param columnsAndTypes the columnsAndTypes to set
	 */
	public void setColumnsAndTypes(LinkedHashMap<String, String> columnsAndTypes) {
		this.columnsAndTypes = columnsAndTypes;
	}

	/**
	 * @return the primaryKey
	 */
	public List<String> getPrimaryKey() {
		return primaryKey;
	}

	/**
	 * @param primaryKey the primaryKey to set
	 */
	public void setPrimaryKey(List<String> primaryKey) {
		this.primaryKey = primaryKey;
	}

	/**
	 * @return the numberOfRows
	 */
	public long getNumberOfRows() {
		return numberOfRows;
	}

	/**
	 * @param numberOfRows the numberOfRows to set
	 */
	public void setNumberOfRows(long numberOfRows) {
		this.numberOfRows = numberOfRows;
	}
}

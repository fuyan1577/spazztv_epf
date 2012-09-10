/**
 * 
 */
package com.spazzmania.epf.ingester;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;

import com.jolbox.bonecp.BoneCP;

/**
 * Abstract class for creating, dropping, merging and updating an EPF Database.
 * <p/>
 * The methods defined here are based on the original Apple EPF Python scripts
 * intended for a MySQL MyISAM database. To understand the methods defined here,
 * a description of the original Python scrips is necessary. The following logic
 * was employed:
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
 * <li/>public void createTable(List<String,String> columnsAndTypes);
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
public abstract class EPFDbUtility {

	private BoneCP connector;
	private String schema;

	public EPFDbUtility(BoneCP connector, String schema) {
		this.connector = connector;
		this.schema = schema;
	}

	/**
	 * Initialize the table to be imported based on the importType and number of
	 * records.
	 * 
	 * @param importType
	 *            - Enum <i>FULL</i> or <i>INCREMENTAL</i>
	 * @param tableName
	 *            - Name of the table to be created from the import
	 * @param numberOfRows
	 *            - Number of rows in the import file
	 */
	public abstract void initImport(EPFExportType exportType, String tableName,
			long numberOfRows);

	/**
	 * A LinkedHashMap of Column Name & Column Type pairs.
	 * 
	 * @param columnsAndTypes
	 */
	public abstract void createTable(
			LinkedHashMap<String, String> columnsAndTypes);

	/**
	 * Set the column that is the Primary Key. Single column primary keys are
	 * assumed.
	 * 
	 * @param columnName
	 */
	public abstract void setPrimaryKey(String[] columnName);

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
	public abstract void insertRow(String[] rowData);

	/**
	 * Finalize any table insert optimization. The original EPF Python script
	 * logic would insert any remaining queued rows and possibly merge, drop and
	 * rename tables.
	 */
	public abstract void finalizeImport();
	
	public Connection getConnection() throws SQLException {
		return connector.getConnection();
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
	public boolean isTableInDatabase(String tableName) {
		DatabaseMetaData dbm;
		try {
			Connection connection = getConnection();
			dbm = connection.getMetaData();
			String types[] = { "TABLE" };
			ResultSet tables = dbm.getTables(null, schema, tableName, types);
			tables.beforeFirst();
			if (tables.next()) {
				return true;
			}
			connection.close();
		} catch (SQLException e) {
			// IGNORE - an error will occur when the table doesn't exist
		}
		return false;
	}

	/**
	 * Returns the number of columns for <i>tableName</i>.
	 * <p/>
	 * Used for <i>INCREMENTAL</i> imports when determining whether to continue
	 * with the imports.
	 * 
	 * @param tableName
	 * @return the number of columns
	 */
	public int getTableColumnCount(String tableName) {
		DatabaseMetaData dbm;
		try {
			Connection connection = getConnection();
			dbm = connection.getMetaData();
			// Get the list of table columns as a SQL Result Set
			ResultSet columns = dbm.getColumns(null, schema, tableName, null);
			columns.last(); // Move to the last row of the result set
			int columnCount = columns.getRow();
			connection.close();
			return columnCount; // return the row number as the column count
		} catch (SQLException e) {
			// IGNORE - an error will occur when the table doesn't exist
		}
		return 0;
	}
}

/**
 * 
 */
package com.spazzmania.epf.ingester;

import java.util.LinkedHashMap;

/**
 * Interface for creating, dropping, merging and updating an EPF Database.
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
 * Because the original script followed this logic, this interface requires the
 * following methods:
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
 * 
 * @author Thomas Billingsley
 * 
 */
public interface EPFDbUtility {

	public enum EPFImportType {
		FULL, INCREMENTAL
	};

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
	public void initImport(EPFImportType importType, String tableName,
			long numberOfRows);

	/**
	 * A LinkedHashMap of Column Name & Column Type pairs.
	 * 
	 * @param columnsAndTypes
	 */
	public void createTable(LinkedHashMap<String, String> columnsAndTypes);

	/**
	 * Set the column that is the Primary Key. Single column primary keys are
	 * assumed.
	 * 
	 * @param columnName
	 */
	public void setPrimaryKey(String columnName);

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
	public void insertRow(String[] rowData);

	/**
	 * Finalize any table insert optimization. The original EPF Python script
	 * logic would insert any remaining queued rows and possibly merge, drop and
	 * rename tables.
	 */
	public void finalizeImport();
}

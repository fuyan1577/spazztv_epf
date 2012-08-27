package com.spazzmania.epf.ingester;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;

public class EPFDbUtilityMySQL implements EPFDbUtility {

	private Connection connection;

	public static long MERGE_THRESHOLD = 500000;
	public static long INSERT_BUFFER_SIZE = 200;
	
	private String tableName;
	private String impTableName;
	private String uncTableName;
	private int insertBuffer = 0;
	LinkedHashMap<String, String> columnsAndTypes;

	private enum ProcessMode {IMPORT_RENAME, APPEND, MERGE_RENAME};
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
	
	private void dropTable(String tableName) {
		Statement stmt;
		try {
			stmt = connection.createStatement();
			stmt.execute("drop table if exists `" + tableName + "`");
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void createTable(LinkedHashMap<String, String> columnsAndTypes) {
		this.columnsAndTypes = columnsAndTypes;
		// TODO Auto-generated method stub

	}

	@Override
	public void setPrimaryKey(String columnName) {
		// TODO Auto-generated method stub

	}

	@Override
	public void insertRow(String[] rowData) {
		// TODO Auto-generated method stub

	}

	@Override
	public void finalizeImport() {
		// TODO Auto-generated method stub

	}

}

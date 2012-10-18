package com.spazztv.epf.adapter;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.spazztv.epf.dao.EPFDbException;
import com.spazztv.epf.dao.EPFDbWriter;


public class EPFDbWriterOracleDao {

	public static String COLUMN_NAMES_SQL = "SELECT * FROM \"%s\" WHERE ROWNUM = 1";

	private EPFDbWriter dbWriter;
	
	public EPFDbWriterOracleDao(EPFDbWriter dbWriter) {
		super();
		this.dbWriter = dbWriter;
	}

	public SQLReturnStatus executeSQLStatement(String sqlStmt)
			throws EPFDbException {
		SQLReturnStatus sqlStatus = new SQLReturnStatus();

		Connection connection = null;

		Statement st;
		try {
			connection = dbWriter.getConnection();
			st = connection.createStatement();
			st.execute(sqlStmt);
			sqlStatus.setSuccess(true);
		} catch (SQLException e1) {
			sqlStatus.setSuccess(false);
			sqlStatus.setSqlStateCode(SQL92StateCode.getSQLStateCode(e1
					.getSQLState()));
			sqlStatus.setSqlExceptionCode(e1.getErrorCode());
		}
		dbWriter.releaseConnection(connection);
		return sqlStatus;
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
	public List<String> getTableColumns(String tableName) throws EPFDbException {
		Connection connection = null;

		String sqlStmt = String.format(COLUMN_NAMES_SQL, tableName);
		List<String> tableColumns = new ArrayList<String>();
		Statement st;

		try {
			connection = dbWriter.getConnection();
			st = connection.createStatement();
			ResultSet resultSet = st.executeQuery(sqlStmt);
			ResultSetMetaData metaData = resultSet.getMetaData();
			for (int i = 0; i < metaData.getColumnCount(); i++) {
				tableColumns.add(metaData.getColumnName(i));
			}
		} catch (Exception e) {
			// Ignore all errors & exceptions...
			// If the table does not exist because it hasn't
			// previously been imported, return an empty tableColumns list.
		} catch (Error e) {
		}

		dbWriter.releaseConnection(connection);

		return tableColumns;
	}

	public boolean isTableInDatabase(String tableName) throws EPFDbException {
		DatabaseMetaData dbm;
		Connection connection = null;
		try {
			connection = dbWriter.getConnection();
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
			dbWriter.releaseConnection(connection);
		}

		return false;
	}
}

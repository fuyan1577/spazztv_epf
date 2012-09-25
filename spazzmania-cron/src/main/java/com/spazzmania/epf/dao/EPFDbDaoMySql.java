package com.spazzmania.epf.dao;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.spazzmania.epf.ingester.SQL92StateCode;

public class EPFDbDaoMySql extends EPFDbDao {

	public static String COLUMN_NAMES_SQL = "SELECT * FROM `%s` LIMIT 1";

	public SQLReturnStatus executeSQLStatement(String sqlStmt)
			throws EPFDbException {
		SQLReturnStatus sqlStatus = new SQLReturnStatus();

		Connection connection = null;

//		int l = sqlStmt.length();
//		if (l > 80) {
//			l = 80;
//		}
//		System.out.println(sqlStmt.substring(0, l));

		Statement st;
		try {
			connection = getConnector().getConnection();
			st = connection.createStatement();
			st.execute(sqlStmt);
			sqlStatus.setSuccess(true);
		} catch (SQLException e1) {
			sqlStatus.setSuccess(false);
			sqlStatus.setSqlStateCode(SQL92StateCode.getSQLStateCode(e1
					.getSQLState()));
			sqlStatus.setSqlExceptionCode(e1.getErrorCode());
		}
		getConnector().releaseConnection(connection);
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
			connection = getConnector().getConnection();
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

		getConnector().releaseConnection(connection);

		return tableColumns;
	}

	public boolean isTableInDatabase(String tableName) throws EPFDbException {
		DatabaseMetaData dbm;
		Connection connection = null;
		try {
			connection = getConnector().getConnection();
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
			getConnector().releaseConnection(connection);
		}

		return false;
	}
}

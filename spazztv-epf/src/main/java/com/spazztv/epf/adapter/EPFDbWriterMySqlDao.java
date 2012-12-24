package com.spazztv.epf.adapter;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.spazztv.epf.dao.EPFDbException;
import com.spazztv.epf.dao.EPFDbWriter;

public class EPFDbWriterMySqlDao {

	public static String COLUMN_NAMES_SQL = "SELECT * FROM `%s` LIMIT 1";
	private final Charset UTF8_CHARSET = Charset.forName("UTF-8");
	private List<Boolean> charColumns;

	private EPFDbWriter dbWriter;

	public EPFDbWriterMySqlDao(EPFDbWriter dbWriter) {
		super();
		this.dbWriter = dbWriter;
	}

	public SQLReturnStatus executeSQLStatement(String sqlStmt,
			List<List<String>> insertBuffer) throws EPFDbException {
		SQLReturnStatus sqlStatus = new SQLReturnStatus();

		Connection connection = null;
		PreparedStatement ps = null;
		try {
			connection = dbWriter.getConnection();
			ps = connection.prepareStatement(sqlStmt);
			setSqlValues(ps, insertBuffer);
			ps.execute();
			sqlStatus.setSuccess(true);
		} catch (SQLException e1) {
			sqlStatus.setSuccess(false);
			sqlStatus.setSqlState(e1.getSQLState());
			sqlStatus.setSqlExceptionCode(e1.getErrorCode());
			sqlStatus.setDescription(e1.getMessage());
		} finally {
			try {
				if (ps != null)
					ps.close();
			} catch (SQLException e) {
				// Ignore
			}
		}
		dbWriter.releaseConnection(connection);
		return sqlStatus;
	}

	private void setSqlValues(PreparedStatement ps,
			List<List<String>> insertBuffer) throws SQLException {
		if (insertBuffer == null) {
			return;
		}
		if (charColumns == null) {
			loadCharColumns();
		}
		int i = 1;
		int c = 0;
		for (List<String> rowValues : insertBuffer) {
			c = 0;
			for (String fieldValue : rowValues) {
				ps.setString(i, null);
				if (fieldValue != null) {
					if (fieldValue.length() > 0) {
						if (isCharColumn(c)) {
							ps.setBytes(i, fieldValue.getBytes(UTF8_CHARSET));
						} else {
							ps.setString(i, fieldValue);
						}
					}
				}
				i++;
				c++;
			}
		}
	}

	private void loadCharColumns() throws SQLException {
		if (dbWriter.getColumnsAndTypes() == null) {
			throw new SQLException("Invalid columnsAndTypes array");
		}
		charColumns = new ArrayList<Boolean>();
		for (String columnType : dbWriter.getColumnsAndTypes().values()) {
			if (columnType.matches(".*CHAR.*")
					|| columnType.matches(".*TEXT.*")) {
				charColumns.add(new Boolean(true));
			} else {
				charColumns.add(new Boolean(false));
			}
		}
	}

	private boolean isCharColumn(int c) {
		return charColumns.get(c);
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

	/**
	 * Convenience method returns the dbWriter's tableName property.
	 * <p>
	 * This method is used by Logger Aspects
	 * 
	 * @return the tableName of this object
	 */
	public String getTableName() {
		if (dbWriter != null) {
			return dbWriter.getTableName();
		}
		return null;
	}
}

/**
 * 
 */
package com.spazzmania.model.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * @author tjbillingsley
 * 
 */
public class SpazzDBUtil {

	private String username;
	private String password;
	private String host;
	private String database;

	public SpazzDBUtil(String host, String database, String username,
			String password) {
		this.username = username;
		this.password = password;
		this.host = host;
		this.database = database;
	}

	private Connection getConnection() {
		Connection connection = null;
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			String url = String.format("jdbc:mysql://%s/%s", host, database);
			connection = DriverManager.getConnection(url, username, password);
		} catch (ClassNotFoundException ex) {
			throw new RuntimeException(ex);
		} catch (IllegalAccessException ex) {
			throw new RuntimeException(ex);
		} catch (InstantiationException ex) {
			throw new RuntimeException(ex);
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
		return connection;
	}

	public String getKeyValue(String key) {
		Connection dbConn = getConnection();
		String val = null;
		try {
			Statement st = dbConn.createStatement();
			String sql = String
					.format("SELECT `value` from spazzmania.system_key_values where `key` = '%s'",
							key);
			ResultSet rs = st.executeQuery(sql);
			if (rs.next()) {
				val = rs.getString("value");
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (dbConn != null) {
					dbConn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
				throw new RuntimeException("Unable to close db Connection");
			}
		}

		return val;
	}

	public void setKeyValue(String key, String value) {
		Connection dbConn = getConnection();
		try {
			Statement st = dbConn.createStatement();
			String readSql = String
					.format("SELECT `value` from spazzmania.system_key_values where `key` = '%s'",
							key);
			ResultSet rs = st.executeQuery(readSql);
			String sql = String
					.format("INSERT into spazzmania.system_key_values (`key`,`value`) values ('%s','%s')",
							key, value);
			if (rs.next()) {
				sql = String
						.format("UPDATE spazzmania.system_key_values set `value` = '%s' where `key` = '%s'",
								value, key);
			}
			st.executeUpdate(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (dbConn != null) {
					dbConn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
				throw new RuntimeException("Unable to close db Connection");
			}
		}
	}

	public boolean executeScript(String sql) {
		return false;
	}

	public List<String> splitSqlScript(String sql, String delimiter) {
		return null;
	}
}

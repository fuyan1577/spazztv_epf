/**
 * 
 */
package com.spazzmania.model.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

	public boolean executeScript(String sql, String sqldDelimiter) {
		List<String> sqlScript = splitSqlScript(sql,sqldDelimiter);
		for (String sqlStmt : sqlScript) {
			Connection dbConn = getConnection();
			try {
				Statement st = dbConn.createStatement();
				st.execute(sqlStmt);
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
		return true;
	}

	/**
	 * Trim the block of SQL using the following steps:
	 * <ol>
	 * <li>Remove all lines beginning with a SQL comment: <code>-- comment</code>
	 * <li>Replace all Line Feeds & Carriage Return characters with spaces
	 * <li>Reduce recurring spaces to one space
	 * <li>Split the SQL Script by the <code>sqlDelimiter</code> argument
	 * <li>Remove the last blank line
	 * </ol>
	 * @param sql
	 * @param sqlDelimiter
	 * @return
	 */
	public List<String> splitSqlScript(String sql, String sqlDelimiter) {
		//Remove the comments
		Pattern stripComments = Pattern.compile("^--.+$",Pattern.MULTILINE);
		Matcher sc = stripComments.matcher(sql);
		sql = sc.replaceAll("");

		//Pattern to remove the line feeds
		Pattern lf = Pattern.compile("\\n");
		//Pattern to remove the carriage returns
		Pattern cr = Pattern.compile("\\r");
		//Pattern to split comamnds by sqlDelimiter
		Pattern sqlSplitter = Pattern.compile(sqlDelimiter);
		//Execute the Patterns
		sql = lf.matcher(sql).replaceAll(" ");
		sql = cr.matcher(sql).replaceAll(" ");
		//Reduce multiple space to one space
		sql = sql.replaceAll("\\s+"," ");
		
		//Split the SQL script by delimiter
		String[] sqlBlock = sqlSplitter.split(sql);

		//Remove the extraneous last line
		String[] sqlTrimmed;
		if (sqlBlock.length > 1) {
			sqlTrimmed = new String[sqlBlock.length-1];
			System.arraycopy(sqlBlock, 0, sqlTrimmed, 0, sqlBlock.length - 1);
		}
		else {
			sqlTrimmed = sqlBlock;
		}
		
		//Return as a List<String>
		return Arrays.asList(sqlTrimmed);
	}
}

package com.spazztv.epf.dao;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class MySqlPreparedStatementTest {

	public static String CREATE_TABLE_STMT = "CREATE TABLE %s (%s) ENGINE=MyISAM DEFAULT CHARSET=utf8";

	private EPFDbConnector connector;
	private EPFDbConfig dbConfig;

	public MySqlPreparedStatementTest() {
		dbConfig = new EPFDbConfig();
		dbConfig.setDbUrl("jdbc:mysql://localhost/epf");
		dbConfig.setDbWriterClass("com.spazztv.epf.adapter.EPFDbWriterMySql");
		dbConfig.setDbDataSourceClass("com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
		dbConfig.setMinConnections(5);
		dbConfig.setMaxConnections(20);
		dbConfig.setUsername("epftest");
		dbConfig.setPassword("epftest");
		connector = EPFDbConnector.getInstance(dbConfig);
	}

	public void sqlStatementTest() throws EPFDbException {

		Connection connection = null;
		try {
			connection = connector.getConnection();
			Statement st = connection.createStatement();
			st.execute(getDropTable());
			st.execute(getCreateTable());
			PreparedStatement ps = connection
					.prepareStatement("insert into epf.test (`id`,`field1`,`total`,`create_dt_tm`) values (?, ?, ?, ?)");
			int i;
			for (i = 1; i < 10; i++) {
				ps.setString(1, Integer.toString(i));
				ps.setString(2, String.format("value %d", i));
				ps.setString(3, Integer.toString(i * 1234));
				ps.setString(4,
						(new Timestamp(System.currentTimeMillis()).toString()));
				ps.execute();
			}
			// UTF-8 Test
			ps.setString(1, Integer.toString(i));
			String utf8String = String.format("Οδυσσέα %d", i);
			utf8String = "Οδυσσέα 10";
			ps.setString(2, utf8String);
			ps.setString(3, Integer.toString(i * 1234));
			ps.setString(4,
					(new Timestamp(System.currentTimeMillis()).toString()));
			ps.execute();

		} catch (SQLException e) {
			e.printStackTrace();
		}
		connector.releaseConnection(connection);
	}

	public void sqlStatementTest2() throws EPFDbException {
		Connection connection = null;
		try {
			connection = connector.getConnection();
			PreparedStatement ps = connection
					.prepareStatement("insert into epf.test (`id`,`field1`,`total`,`create_dt_tm`) values (?,?,?,?),(?,?,?,?),(?,?,?,?),(?,?,?,?),(?,?,?,?)");
			for (int i = 0; i < 5; i++) {
				int o = i * 4;
				ps.setString(o + 1, Integer.toString(i + 1001));
				ps.setString(o + 2, String.format("value\n %d", o + 1));
				ps.setString(o + 3, Integer.toString((o + 1) * 123));
				ps.setString(o + 4,
						(new Timestamp(System.currentTimeMillis()).toString()));
			}
			ps.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		connector.releaseConnection(connection);
	}

	public void sqlStatementTest3() {
		BufferedReader bFile = null;
		String filePath = "testdata/utf8_test.dat";
		String characterEncoding = "UTF-8";
		FileInputStream fstream;
		Connection connection = null;
		try {
			fstream = new FileInputStream(filePath);
			DataInputStream in = new DataInputStream(fstream);
			bFile = new BufferedReader(new InputStreamReader(in,
					characterEncoding));
		} catch (Exception e) {
			e.printStackTrace();
		}
		while (true) {
			try {
				String row = bFile.readLine();
				if (row == null) {
					break;
				}
				if (row.length() < 1) {
					break;
				}
				List<String> rowFields = Arrays.asList(row.split(String
						.valueOf('\t')));
				connection = connector.getConnection();
				PreparedStatement ps = connection
						.prepareStatement("insert into epf.test (`id`,`field1`,`total`,`create_dt_tm`) values (?,?,?,?)");
				ps.setString(1, rowFields.get(0));
				ps.setString(2, rowFields.get(1));
				ps.setString(3, rowFields.get(2));
				ps.setString(4,
						(new Timestamp(System.currentTimeMillis()).toString()));
				ps.execute();

			} catch (IOException e) {
				break;
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (EPFDbException e) {
				e.printStackTrace();
			}
		}
	}

	private String getDropTable() {
		return "drop table if exists epf.test";
	}

	private String getCreateTable() {
		return String
				.format(CREATE_TABLE_STMT,
						"epf.test",
						"`id` integer, `field1` varchar(255), `total` integer, `create_dt_tm` timestamp");
	}

	public static void main(String[] args) throws EPFDbException {
		MySqlPreparedStatementTest test = new MySqlPreparedStatementTest();
		test.sqlStatementTest();
		test.sqlStatementTest2();
		test.sqlStatementTest3();
	}
}

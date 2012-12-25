package com.spazztv.epf.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.spazztv.epf.SimpleEPFFileReader;
import com.spazztv.epf.EPFImportTranslator;

public class MySqlUTF8Test {

	public static String CREATE_TABLE_STMT = "CREATE TABLE %s (%s) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4";

	private EPFDbConnector connector;
	private EPFDbConfig dbConfig;
	private List<String> applicationRecord;
	private String filePath = "testdata/utf8_test_longtext.dat";
	private String fieldSeparator = "&#0001;";
	private String recordSeparator = "&#0002;";
	private String insertStmt = "insert into epf.test_utf8 ( `export_date`, `application_id`, `title`, `recommended_age`, `artist_name`, `seller_name`, `company_url`, `support_url`, `view_url`, `artwork_url_large`, `artwork_url_small`, `itunes_release_date`, `copyright`, `description`, `version`, `itunes_version`, `download_size`) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

	public MySqlUTF8Test() {
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

	public void loadApplicationRecord() {
		try {
			EPFImportTranslator importTranslator = new EPFImportTranslator(
					new SimpleEPFFileReader(filePath, fieldSeparator, recordSeparator));
			importTranslator.getColumnAndTypes();
			importTranslator.getTotalExpectedRecords();
			applicationRecord = importTranslator.nextRecord();
			String createDate = applicationRecord.get(11).replaceAll(" ","-");
			applicationRecord.set(11,createDate);
			importTranslator.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void setup() throws EPFDbException {
		loadApplicationRecord();
		
		Connection connection = null;
		try {
			connection = connector.getConnection();
			Statement st = connection.createStatement();
			st.execute(getDropTable());
			st.execute(getCreateTable());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		connector.releaseConnection(connection);
	}

	public void sqlStatementTest() throws EPFDbException {
		Connection connection = null;
		try {
			connection = connector.getConnection();
			PreparedStatement ps = connection
					.prepareStatement(insertStmt);
			for (int i = 0; i < applicationRecord.size(); i++) {
				ps.setString(i+1, applicationRecord.get(i));
			}
			ps.execute();
			ps.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		connector.releaseConnection(connection);
	}

	private String getDropTable() {
		return "drop table if exists epf.test_utf8";
	}

	private String getCreateTable() {
		return "CREATE TABLE epf.test_utf8 (   `export_date` bigint(20) DEFAULT NULL,   `application_id` int(11) NOT NULL DEFAULT '0',   `title` varchar(1000) DEFAULT NULL,   `recommended_age` varchar(20) DEFAULT NULL,   `artist_name` varchar(1000) DEFAULT NULL,   `seller_name` varchar(1000) DEFAULT NULL,   `company_url` varchar(1000) DEFAULT NULL,   `support_url` varchar(1000) DEFAULT NULL,   `view_url` varchar(1000) DEFAULT NULL,   `artwork_url_large` varchar(1000) DEFAULT NULL,   `artwork_url_small` varchar(1000) DEFAULT NULL,   `itunes_release_date` datetime DEFAULT NULL,   `copyright` varchar(4000) DEFAULT NULL,   `description` longtext,   `version` varchar(100) DEFAULT NULL,   `itunes_version` varchar(100) DEFAULT NULL,   `download_size` bigint(20) DEFAULT NULL,   PRIMARY KEY (`application_id`) ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4";
	}

	public static void main(String[] args) throws EPFDbException {
		MySqlUTF8Test test = new MySqlUTF8Test();
		test.setup();
		test.sqlStatementTest();
	}
}

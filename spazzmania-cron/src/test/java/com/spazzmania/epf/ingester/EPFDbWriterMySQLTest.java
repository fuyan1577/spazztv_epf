package com.spazzmania.epf.ingester;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class EPFDbWriterMySQLTest {
	
	EPFDbWriter dbWriter;
	EPFDbConnector connector;
	Connection connection;
	Statement statement;
	String tablePrefix = "test_";
	String tableName = "epftable";
	LinkedHashMap<String,String> columnsAndTypes;

	@Before
	public void setUp() throws Exception {
		connector = EasyMock.createMock(EPFDbConnector.class);
		connection = EasyMock.createMock(Connection.class);
		statement = EasyMock.createMock(Statement.class);
		dbWriter = new EPFDbWriterMySQL();
		dbWriter.setConnector(connector);
		dbWriter.setTablePrefix(tablePrefix);
		
		columnsAndTypes = new LinkedHashMap<String,String>();
		columnsAndTypes.put("export_date","BIGINT");
		columnsAndTypes.put("application_id","INTEGER");
		columnsAndTypes.put("title","VARCHAR(1000)");
		columnsAndTypes.put("recommended_age","VARCHAR(20)");
		columnsAndTypes.put("artist_name","VARCHAR(1000)");
		columnsAndTypes.put("seller_name","VARCHAR(1000)");
		columnsAndTypes.put("company_url","VARCHAR(1000)");
		columnsAndTypes.put("support_url","VARCHAR(1000)");
		columnsAndTypes.put("view_url","VARCHAR(1000)");
		columnsAndTypes.put("artwork_url_large","VARCHAR(1000)");
		columnsAndTypes.put("artwork_url_small","VARCHAR(1000)");
		columnsAndTypes.put("itunes_release_date","DATETIME");
		columnsAndTypes.put("copyright","VARCHAR(4000)");
		columnsAndTypes.put("description","LONGTEXT");
		columnsAndTypes.put("version","VARCHAR(100)");
		columnsAndTypes.put("itunes_version","VARCHAR(100)");
		columnsAndTypes.put("download_size","BIGINT");
	}

	@Test
	public void testInitImport1() throws EPFDbException, SQLException {
		EasyMock.reset(connector);
		connector.getConnection();
		EasyMock.expectLastCall().andReturn(connection).times(7);
		EasyMock.replay(connector);
		EasyMock.reset(connection);
		connection.createStatement();
		EasyMock.expectLastCall().andReturn(statement).times(7);
		connection.close();
		EasyMock.expectLastCall().times(7);
		EasyMock.replay(connection);
		EasyMock.reset(statement);
		statement.execute((String)EasyMock.anyObject());
		EasyMock.expectLastCall().andReturn(true).times(7);
		EasyMock.replay(statement);
		
		long numberOfRows = 500000;
		dbWriter.initImport(EPFExportType.FULL, tableName, columnsAndTypes, numberOfRows);
	}

	@Test
	public void testSetPrimaryKey() {
		fail("Not yet implemented");
	}

	@Test
	public void testInsertRow() {
		fail("Not yet implemented");
	}

	@Test
	public void testFinalizeImport() {
		fail("Not yet implemented");
	}

}

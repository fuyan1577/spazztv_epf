package com.spazztv.epf.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.spazztv.epf.EPFExportType;

public class EPFDbWriterTest {
	
	EPFDbWriter dbWriter;
	EPFDbConnector dbConnector;
	Connection connection;
	
	class EPFDbWriterTestImpl extends EPFDbWriter {
		@Override
		public void initImport(EPFExportType exportType, String tableName,
				LinkedHashMap<String, String> columnsAndTypes, List<String> primaryKey, long numberOfRows)
				throws EPFDbException {
		}

		@Override
		public void insertRow(String[] rowData) throws EPFDbException {
		}

		@Override
		public void finalizeImport() throws EPFDbException {
		}

		@Override
		public boolean isTableInDatabase(String tableName)
				throws EPFDbException {
			return false;
		}

		@Override
		public int getTableColumnCount(String tableName)
				throws EPFDbException {
			return 0;
		}

		@Override
		public Long getTotalRowsInserted() {
			return 0L;
		}
	}

	@Before
	public void setUp() {
		dbConnector = EasyMock.createMock(EPFDbConnector.class);
		dbWriter = new EPFDbWriterTestImpl();
		dbWriter.setConnector(dbConnector);
		connection = EasyMock.createMock(Connection.class);
	}
	
	@Test
	public void testReleaseConnection() throws SQLException, EPFDbException {
		EasyMock.reset(dbConnector);
		dbConnector.releaseConnection(connection);
		EasyMock.expectLastCall().times(1);
		EasyMock.replay(dbConnector);
		
		dbWriter.releaseConnection(connection);
		
		EasyMock.verify(dbConnector);
	}

}

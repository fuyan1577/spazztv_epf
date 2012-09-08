package com.spazzmania.epf.ingester;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

public class EPFDbConnectorTest {
	
	private EPFDbConnector connector;
	
	class EPFDbConnectorMock extends EPFDbConnector {
		
		public EPFDbConnectorMock() {
		}

		public EPFDbConnectorMock(String configPath) throws IOException {
			super(configPath);
		}

		@Override
		public void openConnectionPool(String configPath) throws IOException,
				ClassNotFoundException, SQLException {
		}

		@Override
		public void closeConnectionPool() {
		}

		@Override
		public Connection getConnection() throws SQLException {
			return null;
		}
		
	}
	
	@Before
	public void setUp() {
		connector = new EPFDbConnectorMock();
	}

//	@Test
//	public void testEPFDbConnector() {
//		fail("Not yet implemented");
//	}

	@Test
	public void testParseConfiguration() {
		connector.parseConfiguration("EPFDbConnector.json");
		fail("Not yet implemented");
	}

}

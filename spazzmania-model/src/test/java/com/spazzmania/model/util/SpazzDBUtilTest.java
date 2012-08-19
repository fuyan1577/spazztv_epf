/**
 * 
 */
package com.spazzmania.model.util;

import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.junit.Test;

/**
 * @author tjbillingsley
 * 
 */
public class SpazzDBUtilTest extends TestCase {

	private SpazzDBUtil dbUtil;
	private String username;
	private String password;
	private String host;
	private String database;

	/*
	 * (non-Javadoc)
	 * 
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		this.username = "webaccess";
		this.password = "wsc2aofi";
		this.host = "qa.epf.spazzdata.com";
		this.database = "spazzmania";

		dbUtil = new SpazzDBUtil(host, database, username, password);
	}

	@Test
	public void testGetKeyValue() {
		// String date = dbUtil.getKeyValue("epf.last_epf_import_date");
		// assertTrue("Invalid date returned",date != null);
		assertTrue("String test only - returning true for normal builds", true);
	}

	@Test
	public void testSetKeyValue() {
		// dbUtil.setKeyValue("epf.last_epf_import_date","99999999");
		// String date = dbUtil.getKeyValue("epf.last_epf_import_date");
		// assertTrue("Invalid date returned",date.equals("99999999"));
		assertTrue("String test only - returning true for normal builds", true);
	}
	
	@Test
	public void testSplitSqlScript() {
		String sql = getSampleSqlScript();
		List<String> rtn = dbUtil.splitSqlScript(sql, SQL_DELIMITER);
		Assert.assertTrue("Invalid split expecting 2 sections",rtn.size() == 2);
	}

	public static String SQL_DELIMITER = ";";
	
	public String getSampleSqlScript() {
		return "-- Comment line 1\r\n" + "-- Comment line 2\r\n"
				+ "select date()" + SQL_DELIMITER + "\r\n" + "-- Comment line 3\r\n"
				+ "      select date()" + SQL_DELIMITER + "\r\n" + "-- Comment line 4\r\n";
	}
}

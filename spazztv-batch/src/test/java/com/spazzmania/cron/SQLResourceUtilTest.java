package com.spazzmania.cron;

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.spazzmania.sql.util.SQLResourceUtil;

public class SQLResourceUtilTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testLoadScript() {
		String script = SQLResourceUtil
				.loadScript(EPF2SpazzDataUpdateUtil.EPF_UTIL_SQL_PATH + "/"
						+ EPF2SpazzDataUpdateUtil.UPDATE_GAMES);

		Assert.assertNotNull("SQL Resource Not Loaded", script);
	}

	@Test
	public void testLoadScriptList() {
		List<String> fileList = SQLResourceUtil
				.loadScriptList(EPF2SpazzDataUpdateUtil.EPF_UTIL_SQL_PATH);
		
		Assert.assertTrue("EPF Update SQL resource list was empty",fileList.size() > 0);
		
		fileList = SQLResourceUtil
				.loadScriptList(SpazzmaniaCustomGenresUtil.GENRE_SQL_PATH);
		
		Assert.assertTrue("Spazz Custom Genre SQL resource list was empty",fileList.size() > 0);
	}

}

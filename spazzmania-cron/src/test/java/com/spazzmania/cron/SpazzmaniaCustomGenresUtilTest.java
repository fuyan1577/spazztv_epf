package com.spazzmania.cron;

import java.util.List;

import junit.framework.Assert;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.spazzmania.model.util.SpazzDBUtil;

public class SpazzmaniaCustomGenresUtilTest {

	SpazzmaniaCustomGenresUtil customGenresUtil;
	SpazzDBUtil spazzDBUtil;
	
	@Before
	public void setUp() throws Exception {
		spazzDBUtil = EasyMock.createMock(SpazzDBUtil.class);
		customGenresUtil = new SpazzmaniaCustomGenresUtil();
		customGenresUtil.setSpazzDBUtil(spazzDBUtil);
	}

	@Test
	public void testLoadGenreScriptList() {
		List<String> scriptList = customGenresUtil.loadGenreScriptList();
		Assert.assertTrue("Empty scriptList returned",scriptList.size() > 0);
	}

	@Test
	public void testLoadAndExecuteGenreScripts() {
		Assert.fail("Test method not defined");
	}

}

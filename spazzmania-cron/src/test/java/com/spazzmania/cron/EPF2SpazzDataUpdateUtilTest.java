package com.spazzmania.cron;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.spazzmania.model.util.SpazzDBUtil;

public class EPF2SpazzDataUpdateUtilTest {

	EPF2SpazzDataUpdateUtil dataUpdateUtil;
	SpazzDBUtil spazzDBUtil;

	@Before
	public void setUp() throws Exception {
		spazzDBUtil = EasyMock.createMock(SpazzDBUtil.class);
		dataUpdateUtil = new EPF2SpazzDataUpdateUtil();
		dataUpdateUtil.setSpazzDBUtil(spazzDBUtil);
	}

	@Test
	public void testLoadSqlScript() {
		String script = dataUpdateUtil
				.loadSqlScript(EPF2SpazzDataUpdateUtil.EPF_UTIL_SQL_PATH + "/"
						+ EPF2SpazzDataUpdateUtil.UPDATE_GAMES);

		Assert.assertNotNull("SQL Resource Not Loaded", script);
	}

	@Test
	public void testLoadAndExecuteScript() {
		// String sql = dataUpdateUtil
		// .loadSqlScript(EPF2SpazzDataUpdateUtil.EPF_UTIL_SQL_PATH + "/"
		// + EPF2SpazzDataUpdateUtil.UPDATE_GAMES);
		//
		// Pattern stripComments = Pattern.compile("^--.+$",Pattern.MULTILINE);
		// Matcher sc = stripComments.matcher(sql);
		// sql = sc.replaceAll("");
		// sql = sql.replaceAll("\\r"," ");
		// sql = sql.replaceAll("\\n"," ");
		// sql = sql.replaceAll("\\s+"," ");
		// Pattern splitLines = Pattern.compile(";");
		// List<String> sqlScript = Arrays.asList(splitLines.split(sql));
		//
		// Assert.assertNotNull("Wow - this didn't work!!!!", sqlScript);

		spazzDBUtil.executeScript(EasyMock.anyObject(String.class),
				EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(spazzDBUtil);
		dataUpdateUtil
				.loadAndExecuteScript(EPF2SpazzDataUpdateUtil.UPDATE_GAMES);
		EasyMock.verify(spazzDBUtil);
	}

	@Test
	public void testUpdateGames() {
		spazzDBUtil.executeScript(EasyMock.anyObject(String.class),
				EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(spazzDBUtil);
		dataUpdateUtil.updateGames();
		EasyMock.verify(spazzDBUtil);
	}

	@Test
	public void testUpdateGamelistPlatforms() {
		spazzDBUtil.executeScript(EasyMock.anyObject(String.class),
				EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(spazzDBUtil);
		dataUpdateUtil.updateGamelistPlatforms();
		EasyMock.verify(spazzDBUtil);
	}

	@Test
	public void testUpdateGameDetails() {
		spazzDBUtil.executeScript(EasyMock.anyObject(String.class),
				EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(spazzDBUtil);
		dataUpdateUtil.updateGameDetails();
		EasyMock.verify(spazzDBUtil);
	}

	@Test
	public void testUpdateGamePrices() {
		spazzDBUtil.executeScript(EasyMock.anyObject(String.class),
				EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(spazzDBUtil);
		dataUpdateUtil.updateGamePrices();
		EasyMock.verify(spazzDBUtil);
	}

	@Test
	public void testUpdateLimitedTimeOffers() {
		spazzDBUtil.executeScript(EasyMock.anyObject(String.class),
				EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(spazzDBUtil);
		dataUpdateUtil.updateLimitedTimeOffers();
		EasyMock.verify(spazzDBUtil);
	}

	@Test
	public void testUpdateGameGenres() {
		spazzDBUtil.executeScript(EasyMock.anyObject(String.class),
				EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(spazzDBUtil);
		dataUpdateUtil.updateGameGenres();
		EasyMock.verify(spazzDBUtil);
	}

	@Test
	public void testUpdateMasterTables() {
		spazzDBUtil.executeScript(EasyMock.anyObject(String.class),
				EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(spazzDBUtil);
		dataUpdateUtil.updateMasterTables();
		EasyMock.verify(spazzDBUtil);
	}

	@Test
	public void testUpdateDeviceTypesGames() {
		spazzDBUtil.executeScript(EasyMock.anyObject(String.class),
				EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(spazzDBUtil);
		dataUpdateUtil.updateDeviceTypesGames();
		EasyMock.verify(spazzDBUtil);
	}

	@Test
	public void testUpdateGamesGenresColumn() {
		spazzDBUtil.executeScript(EasyMock.anyObject(String.class),
				EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(spazzDBUtil);
		dataUpdateUtil.updateGamesGenresColumn();
		EasyMock.verify(spazzDBUtil);
	}

}

package com.spazzmania.cron;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import com.spazzmania.model.util.SpazzDBUtil;

public class EPF2SpazzDataUpdateUtilTest {

	EPF2SpazzDataUpdateUtil dataUpdateUtil;
	SpazzDBUtil spazzDBUtil;
	Logger logger;

	@Before
	public void setUp() throws Exception {
		spazzDBUtil = EasyMock.createMock(SpazzDBUtil.class);
		logger = EasyMock.createMock(Logger.class);

		dataUpdateUtil = new EPF2SpazzDataUpdateUtil();
		dataUpdateUtil.setSpazzDBUtil(spazzDBUtil);
		dataUpdateUtil.setLogger(logger);
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
		
		logger.info(EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().times(1);
		EasyMock.replay(logger);
		
		dataUpdateUtil.updateGames();
		EasyMock.verify(spazzDBUtil);
		EasyMock.verify(logger);
	}

	@Test
	public void testUpdateGamelistPlatforms() {
		spazzDBUtil.executeScript(EasyMock.anyObject(String.class),
				EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(spazzDBUtil);
		
		logger.info(EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().times(1);
		EasyMock.replay(logger);
		
		dataUpdateUtil.updateGamelistPlatforms();
		EasyMock.verify(spazzDBUtil);
		EasyMock.verify(logger);
	}

	@Test
	public void testUpdateGameDetails() {
		spazzDBUtil.executeScript(EasyMock.anyObject(String.class),
				EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(spazzDBUtil);
		
		logger.info(EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().times(1);
		EasyMock.replay(logger);
		
		dataUpdateUtil.updateGameDetails();
		EasyMock.verify(spazzDBUtil);
		EasyMock.verify(logger);
	}

	@Test
	public void testUpdateGamePrices() {
		spazzDBUtil.executeScript(EasyMock.anyObject(String.class),
				EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(spazzDBUtil);
		
		logger.info(EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().times(1);
		EasyMock.replay(logger);
		
		dataUpdateUtil.updateGamePrices();
		EasyMock.verify(spazzDBUtil);
		EasyMock.verify(logger);
	}

	@Test
	public void testUpdateLimitedTimeOffers() {
		spazzDBUtil.executeScript(EasyMock.anyObject(String.class),
				EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(spazzDBUtil);
		
		logger.info(EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().times(1);
		EasyMock.replay(logger);
		
		dataUpdateUtil.updateLimitedTimeOffers();
		EasyMock.verify(spazzDBUtil);
		EasyMock.verify(logger);
	}

	@Test
	public void testUpdateGameGenres() {
		spazzDBUtil.executeScript(EasyMock.anyObject(String.class),
				EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(spazzDBUtil);
		
		logger.info(EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().times(1);
		EasyMock.replay(logger);
		
		dataUpdateUtil.updateGameGenres();
		EasyMock.verify(spazzDBUtil);
		EasyMock.verify(logger);
	}

	@Test
	public void testUpdateMasterTables() {
		spazzDBUtil.executeScript(EasyMock.anyObject(String.class),
				EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(spazzDBUtil);
		
		logger.info(EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().times(1);
		EasyMock.replay(logger);
		
		dataUpdateUtil.updateMasterTables();
		EasyMock.verify(spazzDBUtil);
		EasyMock.verify(logger);
	}

	@Test
	public void testUpdateDeviceTypesGames() {
		spazzDBUtil.executeScript(EasyMock.anyObject(String.class),
				EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(spazzDBUtil);
		
		logger.info(EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().times(1);
		EasyMock.replay(logger);
		
		dataUpdateUtil.updateDeviceTypesGames();
		EasyMock.verify(spazzDBUtil);
		EasyMock.verify(logger);
	}

	@Test
	public void testUpdateGamesGenresColumn() {
		spazzDBUtil.executeScript(EasyMock.anyObject(String.class),
				EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(spazzDBUtil);
		
		logger.info(EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().times(1);
		EasyMock.replay(logger);
		
		dataUpdateUtil.updateGamesGenresColumn();
		EasyMock.verify(spazzDBUtil);
		EasyMock.verify(logger);
	}

}

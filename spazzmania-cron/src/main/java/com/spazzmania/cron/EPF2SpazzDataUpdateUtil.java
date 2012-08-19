/**
 * 
 */
package com.spazzmania.cron;

import java.io.IOException;
import java.io.InputStream;

import com.spazzmania.model.util.SpazzDBUtil;

/**
 * EPF2SpazzDataUpdateUtil is a series of SQL tasks that update data from the
 * Enterprise Partner Feed Database (EPF DB) to the Spazzmania DB.
 * 
 * <p>
 * The tasks carried out by this Utility are:
 * <ul>
 * <li/>updateGames.sql
 * <li/>updateGamelistPlatforms.sql
 * <li/>updateGameDetails.sql
 * <li/>updateGamePrices.sql
 * <li/>updateLimitedTimeOffers.sql
 * <li/>updateGameGenres.sql
 * <li/>updateMasterTables.sql
 * <li/>updateDeviceTypesGames.sql
 * <li/>updateGamesGenresColumn.sql
 * </ul>
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPF2SpazzDataUpdateUtil {

	//public static String EPF_UTIL_SQL_PATH = "com.spazzmania.epf.sql";
	public static String EPF_UTIL_SQL_PATH = "/com/spazzmania/epf/sql";
	public static String EPF_SQL_DELIMITER = ";";
	public static String UPDATE_GAMES = "updateGames.sql";
	public static String UPDATE_GAMELIST_PLATFORMS = "updateGamelistPlatforms.sql";
	public static String UPDATE_GAME_DETAILS = "updateGameDetails.sql";
	public static String UPDATE_GAME_PRICES = "updateGamePrices.sql";
	public static String UPDATE_LIMITED_TIME_OFFERS = "updateLimitedTimeOffers.sql";
	public static String UPDATE_GAME_GENRES  = "updateGameGenres.sql";
	public static String UPDATE_MASTER_TABLES = "updateMasterTables.sql";
	public static String UPDATE_DEVICE_TYPES_GAMES = "updateDeviceTypesGames.sql"; 
	public static String UPDATE_GAMES_GENRES_COLUMN = "updateGamesGenresColumn.sql";
	private SpazzDBUtil spazzDBUtil;

	/**
	 * @return the spazzDBUtil
	 */
	public SpazzDBUtil getSpazzDBUtil() {
		return spazzDBUtil;
	}

	/**
	 * @param spazzDBUtil the spazzDBUtil to set
	 */
	public void setSpazzDBUtil(SpazzDBUtil spazzDBUtil) {
		this.spazzDBUtil = spazzDBUtil;
	}

	/**
	 * Update the games from EPF Applications
	 */
	public synchronized void updateGames() {
		loadAndExecuteScript(UPDATE_GAMES);
	}
	
	/**
	 * Update the Gamelist Platforms (aka devices).
	 */
	public synchronized void updateGamelistPlatforms() {
		loadAndExecuteScript(UPDATE_GAMELIST_PLATFORMS);
	}

	/**
	 * Update the Game Details from EPF Application Details.
	 */
	public synchronized void updateGameDetails() {
		loadAndExecuteScript(UPDATE_GAME_DETAILS);
	}

	/**
	 * Update the games prices from EPF application_price
	 */
	public synchronized void updateGamePrices() {
		loadAndExecuteScript(UPDATE_GAME_PRICES);
	}

	/**
	 * Update the limited time offers based on text in the game description
	 */
	public synchronized void updateLimitedTimeOffers() {
		loadAndExecuteScript(UPDATE_LIMITED_TIME_OFFERS);
	}

	/**
	 * Update the game genres from EPF genre_application
	 */
	public synchronized void updateGameGenres() {
		loadAndExecuteScript(UPDATE_GAME_GENRES);
	}

	/**
	 * Update the game genres from EPF master tables
	 */
	public synchronized void updateMasterTables() {
		loadAndExecuteScript(UPDATE_MASTER_TABLES);
	}

	/**
	 * Update the game genres from EPF application_device_types
	 */
	public synchronized void updateDeviceTypesGames() {
		loadAndExecuteScript(UPDATE_DEVICE_TYPES_GAMES);
	}

	/**
	 * Update the game genres column
	 */
	public synchronized void updateGamesGenresColumn() {
		loadAndExecuteScript(UPDATE_GAMES_GENRES_COLUMN);
	}
	
	/**
	 * Load a sqlScriptName from a resource and execute the script
	 */
	public void loadAndExecuteScript(String sqlScriptName) {
		String sqlScript = loadSqlScript(EPF_UTIL_SQL_PATH + "/" + sqlScriptName);
		spazzDBUtil.executeScript(sqlScript, EPF_SQL_DELIMITER);
	}

	/**
	 * Load a sql script from a file and return it as an array of SQL comamnds.
	 * 
	 * @param sqlResourceName
	 *            to Load
	 * @return String[] array of SQL commands comprising the script
	 */
	public String loadSqlScript(String sqlResourceName) {
		InputStream sqlFile = EPF2SpazzDataUpdateUtil.class
				.getResourceAsStream(sqlResourceName);

		byte[] buffer = null;
		try {
			buffer = new byte[sqlFile.available()];
			sqlFile.read(buffer, 0, sqlFile.available());
			sqlFile.close();
		} catch (IOException e) {
			// Convert into runtime exception and let the job scheduler handle
			// report it
			throw new RuntimeException(e);
		}
		
		return new String(buffer);
	}
	
}

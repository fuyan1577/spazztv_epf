/**
 * 
 */
package com.spazzmania.cron;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;

import com.spazzmania.model.util.SpazzDBUtil;
import com.spazzmania.sql.util.SQLResourceUtil;

/**
 * This object loads the sql scripts located in the /com/spazzmania/sql/genres
 * path and executes them to create custom genres. Each genre script found in
 * this path is loaded and executed as a scripts. Each script executed is listed
 * in the output job log.
 * <p>
 * This utility is designed to be executed by a job run in a scheduler. All
 * exceptions are converted into RuntimeExceptions for the scheduler.
 * 
 * @author Thomas Billingsley
 * 
 */
public class SpazzmaniaCustomGenresUtil {

	public static String GENRE_SQL_PATH = "/com/spazzmania/sql/genre";
	public static String SQL_DELIMITER = ";";
	private SpazzDBUtil spazzDBUtil;
	private Logger logger;

	/**
	 * @return the spazzDBUtil
	 */
	public SpazzDBUtil getSpazzDBUtil() {
		return spazzDBUtil;
	}

	/**
	 * @param spazzDBUtil
	 *            the spazzDBUtil to set
	 */
	public void setSpazzDBUtil(SpazzDBUtil spazzDBUtil) {
		this.spazzDBUtil = spazzDBUtil;
	}

	public void loadAndExecuteGenreScripts() {
		for (String genreScript : loadGenreScriptList()) {
			logger.info("Running genre script: " + genreScript);
			String sql = SQLResourceUtil.loadScript(genreScript);
			spazzDBUtil.executeScript(sql, SQL_DELIMITER);
		}
	}
	
	public List<String> loadGenreScriptList() {
		return SQLResourceUtil.loadScriptList(GENRE_SQL_PATH);
	}
}

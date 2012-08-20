/**
 * 
 */
package com.spazzmania.sql.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author Thomas Billingsley
 * 
 */
public class SQLResourceUtil {
	/**
	 * Load a SQL script from a file and return it as a String.
	 * 
	 * @param sqlResourceName
	 *            to Load
	 * @return String[] array of SQL commands comprising the script
	 */
	public synchronized static String loadScript(String sqlResourceName) {
		InputStream sqlFile = SQLResourceUtil.class
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

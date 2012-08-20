/**
 * 
 */
package com.spazzmania.sql.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;

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

	/**
	 * List directory contents for a resource folder. Not recursive. This is
	 * basically a brute-force implementation. Works for regular files and also
	 * JARs.
	 * 
	 * @author Greg Briggs
	 * @param path
	 *            Should end with "/", but not start with one.
	 * @return Just the name of each member item, not the full paths.
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	public synchronized static List<String> loadScriptList(String path) {
		try {
			URL dirURL = SQLResourceUtil.class.getResource(path);
			if (dirURL != null && dirURL.getProtocol().equals("file")) {
				/* A file path: easy enough */

				Set<String> resultSet = new TreeSet<String>();
				File fileDir = new File(dirURL.toURI());
				for (int i = 0; i < fileDir.list().length; i++) {
					String entry = fileDir.list()[i];
					if (Pattern.matches(".+\\.sql",entry)) {
						resultSet.add(entry);
					}
				}
				return Arrays.asList(resultSet.toArray(new String[resultSet.size()]));
			}

			if (dirURL == null) {
				return null;
			}

			if (dirURL.getProtocol().equals("jar")) {
				/* A JAR path */
				String jarPath = dirURL.getPath().substring(5,
						dirURL.getPath().indexOf("!")); // strip out only the
														// JAR
														// file
				JarFile jar = new JarFile(URLDecoder.decode(jarPath, "UTF-8"));
				Enumeration<JarEntry> entries = jar.entries(); // gives ALL
																// entries
																// in jar
				Set<String> resultSet = new TreeSet<String>(); // avoid duplicates
															// in
															// case it is a
															// subdirectory
				while (entries.hasMoreElements()) {
					String entry = entries.nextElement().getName();
					if (Pattern.matches(".+\\.sql",entry)) {
						resultSet.add(entry);
					}
				}
				return Arrays.asList(resultSet.toArray(new String[resultSet.size()]));
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return new ArrayList<String>();
	}
	
}

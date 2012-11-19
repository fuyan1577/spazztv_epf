/**
 * 
 */
package com.spazztv.epf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.spazztv.epf.dao.EPFDbConfig;

/**
 * A simple object to parse the EPFConfig.json file and hold the values.
 * 
 * Save my values this time.
 * 
 * @author Thomas Billingsley
 * @version 1.0
 */
public class EPFConfig {

	public static String EPF_BLACKLIST = "blackList";
	public static String EPF_WHITELIST = "whiteList";
	public static String EPF_DIRECTORY_PATHS = "directoryPaths";
	public static String EPF_MAX_THREADS = "maxThreads";
	public static String EPF_ALLOW_EXTENSIONS = "allowExtensions";
	public static String EPF_SKIP_KEY_VIOLATORS = "skipKeyViolators";
	public static String EPF_TABLE_PREFIX = "tablePrefix";
	public static String EPF_RECORD_SEPARATOR = "recordSep";
	public static String EPF_FIELD_SEPARATOR = "fieldSep";

	public static int EPF_MAX_THREADS_DEFAULT = 8;
	public static String EPF_FIELD_SEPARATOR_DEFAULT = "\\x01";
	public static String EPF_RECORD_SEPARATOR_DEFAULT = "\\x02";
	public static String EPF_FLAT_FIELD_SEPARATOR_DEFAULT = "\\t";
	public static String EPF_FLAT_RECORD_SEPARATOR_DEFAULT = "\\n";

	private List<String> whiteList;
	private List<String> blackList;
	private List<String> directoryPaths;
	private int maxThreads = EPF_MAX_THREADS_DEFAULT;
	private boolean allowExtensions = false;
	private boolean skipKeyViolators = false;
	private String tablePrefix = "";
	private String recordSeparator = EPF_RECORD_SEPARATOR_DEFAULT;
	private String fieldSeparator = EPF_FIELD_SEPARATOR_DEFAULT;
	private String snapShotFile;

	public EPFConfig() {
		whiteList = new ArrayList<String>();
		blackList = new ArrayList<String>();

		whiteList.add(".*?");
		blackList.add("^\\.");
		allowExtensions = false;
		tablePrefix = "epf";
	}

	public EPFConfig(File configFile) throws IOException, EPFImporterException {
		this();
		String configJson = loadConfigFile(configFile);
		parseConfiguration(configJson);
	}

	private void checkConfiguration(JSONObject configObj)
			throws EPFImporterException {
		for (Object key : configObj.keySet()) {
			// Skip over any DbConfig keys
			if (!EPFDbConfig.isValidConfigKey((String) key)) {
				if (!isValidConfigKey((String) key)) {
					throw new EPFImporterException("Invalid EPFConfig key: "
							+ key);
				}
				if (JSONObject.class.isAssignableFrom(configObj.get(key)
						.getClass())) {
					checkConfiguration((JSONObject) configObj.get(key));
				}
			}
		}
	}

	private boolean isValidConfigKey(String key) {
		if (key.equals(EPF_ALLOW_EXTENSIONS) || key.equals(EPF_WHITELIST)
				|| key.equals(EPF_BLACKLIST) || key.equals(EPF_DIRECTORY_PATHS)
				|| key.equals(EPF_MAX_THREADS) || key.equals(EPF_WHITELIST)
				|| key.equals(EPF_TABLE_PREFIX)
				|| key.equals(EPF_SKIP_KEY_VIOLATORS)) {
			return true;
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	public void parseConfiguration(String configJson)
			throws EPFImporterException {
		JSONParser parser = new JSONParser();
		JSONObject configObj;
		try {
			configObj = (JSONObject) parser.parse(configJson);
		} catch (ParseException e) {
			throw new EPFImporterException(e.getMessage());
		}

		checkConfiguration(configObj);

		if (configObj.get(EPF_DIRECTORY_PATHS) != null) {
			JSONArray dirPaths = (JSONArray) configObj.get(EPF_DIRECTORY_PATHS);
			directoryPaths = (List<String>)dirPaths;
		} else {
			directoryPaths = new ArrayList<String>();
			directoryPaths.add(System.getProperty("user.dir"));
		}

		if (configObj.get(EPF_ALLOW_EXTENSIONS) != null) {
			allowExtensions = (Boolean) configObj.get(EPF_ALLOW_EXTENSIONS);
		}

		if (configObj.get(EPF_SKIP_KEY_VIOLATORS) != null) {
			skipKeyViolators = (Boolean) configObj.get(EPF_SKIP_KEY_VIOLATORS);
		}

		if (configObj.get(EPF_MAX_THREADS) != null) {
			maxThreads = (Integer) configObj.get(EPF_MAX_THREADS);
		}

		if (configObj.get(EPF_TABLE_PREFIX) != null) {
			tablePrefix = (String) configObj.get(EPF_TABLE_PREFIX);
		}

		checkWhiteList(configObj);
		checkBlackList(configObj);
	}

	private void checkWhiteList(JSONObject configObj) {
		JSONArray wList = (JSONArray) configObj.get(EPF_WHITELIST);
		if (wList == null) {
			return;
		}
		whiteList = new ArrayList<String>();
		for (int i = 0; i < wList.size(); i++) {
			whiteList.add((String) wList.get(i));
		}
	}

	private void checkBlackList(JSONObject configObj) {
		JSONArray bList = (JSONArray) configObj.get(EPF_BLACKLIST);
		if (bList == null) {
			return;
		}
		blackList = new ArrayList<String>();
		for (int i = 0; i < bList.size(); i++) {
			blackList.add((String) bList.get(i));
		}
	}

	private String loadConfigFile(File configFile) throws IOException {
		FileInputStream stream = new FileInputStream(configFile);
		try {
			FileChannel fc = stream.getChannel();
			MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0,
					fc.size());
			/* Instead of using default, pass in a decoder. */
			return Charset.defaultCharset().decode(bb).toString();
		} finally {
			stream.close();
		}
	}

	public List<String> getWhiteList() {
		return whiteList;
	}

	public void addWhiteList(String whiteListItem) {
		blackList.add(whiteListItem);
	}

	public void setWhiteList(List<String> whiteList) {
		this.whiteList = whiteList;
	}

	public List<String> getBlackList() {
		return blackList;
	}

	public void addBlackList(String blackListItem) {
		blackList.add(blackListItem);
	}

	public void setBlackList(List<String> blackList) {
		this.blackList = blackList;
	}

	public List<String> getDirectoryPaths() {
		return directoryPaths;
	}

	public void setDirectoryPaths(List<String> directoryPaths) {
		this.directoryPaths = directoryPaths;
	}

	public int getMaxThreads() {
		return maxThreads;
	}

	public void setMaxThreads(int maxThreads) {
		this.maxThreads = maxThreads;
	}

	public boolean isAllowExtensions() {
		return allowExtensions;
	}

	public void setAllowExtensions(boolean allowExtensions) {
		this.allowExtensions = allowExtensions;
	}

	public boolean isSkipKeyViolators() {
		return skipKeyViolators;
	}

	public void setSkipKeyViolators(boolean skipKeyViolators) {
		this.skipKeyViolators = skipKeyViolators;
	}

	public String getTablePrefix() {
		return tablePrefix;
	}

	public void setTablePrefix(String tablePrefix) {
		this.tablePrefix = tablePrefix;
	}

	public String getRecordSeparator() {
		return recordSeparator;
	}

	public void setRecordSeparator(String recordSeparator) {
		this.recordSeparator = recordSeparator;
	}

	public String getFieldSeparator() {
		return fieldSeparator;
	}

	public void setFieldSeparator(String fieldSeparator) {
		this.fieldSeparator = fieldSeparator;
	}

	public String getSnapShotFile() {
		return snapShotFile;
	}

	public void setSnapShotFile(String snapShotFile) {
		this.snapShotFile = snapShotFile;
	}
}

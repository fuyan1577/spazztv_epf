/**
 * 
 */
package com.spazzmania.epf.importer;

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
	public static String EPF_DIRECTORY_PATH = "directoryPath";
	public static String EPF_MAX_THREADS = "maxThreads";
	public static String EPF_ALLOW_EXTENSIONS = "allowExtensions";
	public static String EPF_SKIP_KEY_VIOLATORS = "skipKeyViolators";

	private List<String> whiteList;
	private List<String> blackList;
	private String directoryPath;
	private int maxThreads;
	private boolean allowExtensions;
	private boolean skipKeyViolators = false;

	public EPFConfig() {
		whiteList = new ArrayList<String>();
		blackList = new ArrayList<String>();
	}

	public EPFConfig(File configFile) throws IOException, EPFImporterException {
		this();
		String configJson = loadConfigFile(configFile);
		parseConfiguration(configJson);
	}

	private void checkConfiguration(JSONObject configObj)
			throws EPFImporterException {
		for (Object key : configObj.keySet()) {
			if (!isValidConfigKey((String)key)) {
				throw new EPFImporterException("Invalid EPFConfig key: " + key);
			}
			if (JSONObject.class
					.isAssignableFrom(configObj.get(key).getClass())) {
				checkConfiguration((JSONObject) configObj.get(key));
			}
		}
	}

	private boolean isValidConfigKey(String key) {
		if (key.equals(EPF_ALLOW_EXTENSIONS) || key.equals(EPF_BLACKLIST)
				|| key.equals(EPF_DIRECTORY_PATH)
				|| key.equals(EPF_MAX_THREADS) || key.equals(EPF_WHITELIST)) {
			return true;
		}
		return false;
	}

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

		// Required Values
		directoryPath = verifyString(configObj, EPF_DIRECTORY_PATH);

		if (configObj.get(EPF_ALLOW_EXTENSIONS) != null) {
			allowExtensions = (Boolean) configObj.get(EPF_ALLOW_EXTENSIONS);
		}

		if (configObj.get(EPF_SKIP_KEY_VIOLATORS) != null) {
			skipKeyViolators = (Boolean) configObj.get(EPF_SKIP_KEY_VIOLATORS);
		}

		checkWhiteList(configObj);
		checkBlackList(configObj);
	}

	private void checkWhiteList(JSONObject configObj) {
		JSONArray wList = (JSONArray) configObj.get(EPF_WHITELIST);
		if (wList == null) {
			return;
		}
		for (int i = 0; i < wList.size(); i++) {
			whiteList.add((String) wList.get(i));
		}
	}

	private void checkBlackList(JSONObject configObj) {
		JSONArray bList = (JSONArray) configObj.get(EPF_BLACKLIST);
		if (bList == null) {
			return;
		}
		for (int i = 0; i < bList.size(); i++) {
			blackList.add((String) bList.get(i));
		}
	}

	private String verifyString(JSONObject connPoolObject, String key) {
		if (connPoolObject.get(key) == null) {
			throw new RuntimeException(String.format(
					"Missing EPFDbConnector Parameter: %s", key));
		}
		return (String) connPoolObject.get(key);
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

	public String getDirectoryPath() {
		return directoryPath;
	}

	public void setDirectoryPath(String directoryPath) {
		this.directoryPath = directoryPath;
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
}

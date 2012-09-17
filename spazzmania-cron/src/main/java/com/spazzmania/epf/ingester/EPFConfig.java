/**
 * 
 */
package com.spazzmania.epf.ingester;

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
 * @author Thomas Billingsley
 * @version 1.0
 */
public class EPFConfig {
	
	public static String EPF_BLACKLIST = "blackList";
	public static String EPF_WHITELIST = "whiteList";
	public static String EPF_TABLE_PREFIX = "tablePrefix";
	public static String EPF_DIRECTORY_PATH = "directoryPath";
	public static String EPF_MAX_THREADS = "maxThreads";
	public static String EPF_ALLOW_EXTENSIONS = "allowExtensions";
	
	private List<String> whiteList;
	private List<String> blackList;
	private String tablePrefix; 
	private String directoryPath;
	private int maxThreads;
	private boolean allowExtensions;
	
	public EPFConfig(String configFilePath) throws IOException {
		whiteList = new ArrayList<String>();
		blackList = new ArrayList<String>();
		String configJson = loadConfigFile(configFilePath);
		parseConfiguration(configJson);
	}
	
	public void parseConfiguration(String configJson) {
		JSONParser parser = new JSONParser();
		JSONObject configObj;
		try {
			configObj = (JSONObject) parser.parse(configJson);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}

		// Required Values
		directoryPath = verifyString(configObj, EPF_DIRECTORY_PATH);

		// Optional values
		tablePrefix = (String)configObj.get(EPF_TABLE_PREFIX);
		
		if (configObj.get(EPF_ALLOW_EXTENSIONS) != null) {
			allowExtensions = Boolean.getBoolean((String)configObj.get(EPF_ALLOW_EXTENSIONS));
		}
		
		checkWhiteList(configObj);
		checkBlackList(configObj);
	}
	
	private void checkWhiteList(JSONObject configObj) {
		JSONArray wList = (JSONArray)configObj.get(EPF_WHITELIST);
		if (wList == null) {
			return;
		}
		for (int i = 0; i < wList.size(); i++) {
			whiteList.add((String)wList.get(i));
		}
	}
	
	private void checkBlackList(JSONObject configObj) {
		JSONArray bList = (JSONArray)configObj.get(EPF_BLACKLIST);
		if (bList == null) {
			return;
		}
		for (int i = 0; i < bList.size(); i++) {
			blackList.add((String)bList.get(i));
		}
	}

	private String verifyString(JSONObject connPoolObject, String key) {
		if (connPoolObject.get(key) == null) {
			throw new RuntimeException(String.format(
					"Missing EPFDbConnector Parameter: %s", key));
		}
		return (String) connPoolObject.get(key);
	}

	private String loadConfigFile(String configFilePath) throws IOException {
		FileInputStream stream = new FileInputStream(new File(configFilePath));
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
	public String getTablePrefix() {
		return tablePrefix;
	}
	public void setTablePrefix(String tablePrefix) {
		this.tablePrefix = tablePrefix;
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
}

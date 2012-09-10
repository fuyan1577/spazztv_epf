/**
 * 
 */
package com.spazzmania.epf.ingester;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This is the work manager class of the EPFImport process that
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFImportManager {
	private EPFDbConnector connector;
	private EPFConfig config;
	private ExecutorService service;
	private List<String> fileList;
	private String whiteListRegex;
	private String blackListRegex;

	public EPFImportManager(EPFConfig config, EPFDbConfig dbConfig) {
		this.config = config;
		connector = new EPFDbConnector(dbConfig);
		service = Executors.newFixedThreadPool(config.getMaxThreads());
		whiteListRegex = createRegexPattern(config.getWhiteList());
		blackListRegex = createRegexPattern(config.getBlackList());
		loadImportFileList();
		loadImportThreads();
	}

	private String createRegexPattern(List<String> list) {
		String regexPattern = null;
		if (list.size() > 0) {
			regexPattern = "";
			Iterator<String> i = list.iterator();
			while (i.hasNext()) {
				String wItem = i.next();
				regexPattern += wItem;
				if (i.hasNext()) {
					regexPattern += "|";
				}
			}
		}
		return "^" + regexPattern + "$";
	}

	public void loadImportFileList() {
		fileList = new ArrayList<String>();
		File dir = new File(config.getDirectoryPath());

		String[] children = dir.list();
		if (children != null) {
			for (int i = 0; i < children.length; i++) {
				// Get filename of file or directory
				if (isValidImportFile(children[i])) {
					fileList.add(children[i]);
				}
			}
		}
	}

	/**
	 * EPF Import File WhiteList & BlackList filter.
	 * <p/>
	 * The BlackList overrides the WhiteList.
	 * <p/>
	 * On instantiation of this object, the WhiteList and BlackList Lists are
	 * converted into regex patterns. The test is done with the following steps:
	 * <ol>
	 * <li/>The importFile will be included by default
	 * <li/>If the WhiteList is not empty, include importFile only if it matches
	 * white list pattern
	 * <li/>If the BlackList is not empty, exclude importFile if it matches the
	 * black list pattern
	 * </ol>
	 * 
	 * @param importFile
	 * @return
	 */
	public boolean isValidImportFile(String importFile) {
		boolean valid = true;
		if (whiteListRegex != null) {
			valid = false;
			if (importFile.matches(whiteListRegex)) {
				valid = true;
			}
		}
		if (blackListRegex != null) {
			if (importFile.matches(blackListRegex)) {
				valid = false;
			}
		}
		return valid;
	}

	public void loadImportThreads() {
		for (String filePath : fileList) {
			service.submit(new EPFImportTask(null,null));
		}
	}
}

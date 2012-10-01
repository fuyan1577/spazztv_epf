/**
 * 
 */
package com.spazzmania.epf.importer;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.spazzmania.epf.dao.EPFDbConfig;
import com.spazzmania.epf.dao.EPFDbException;
import com.spazzmania.epf.dao.EPFDbWriter;
import com.spazzmania.epf.dao.EPFDbWriterFactory;

/**
 * This is the main work manager class of the EPFImport process that queues all
 * EPFImportTasks.
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFImportManager {
	private ExecutorService service;
	private List<String> fileList;
	private String whiteListRegex;
	private String blackListRegex;
	private List<Future<Runnable>> importThreads;

	public EPFImportManager() {
		super();
	}
	
	public EPFImportManager(EPFConfig config, EPFDbConfig dbConfig) {
		service = Executors.newFixedThreadPool(config.getMaxThreads());
		
		whiteListRegex = createRegexPattern(config.getWhiteList());
		blackListRegex = createRegexPattern(config.getBlackList());
		loadImportFileList(config.getDirectoryPath());
		loadImportThreads(dbConfig);
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

	public void loadImportFileList(String directoryPath) {
		fileList = new ArrayList<String>();
		File dir = new File(directoryPath);

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

	@SuppressWarnings("unchecked")
	public void loadImportThreads(EPFDbConfig dbConfig) {
		for (String filePath : fileList) {
			try {
				EPFFileReader fileReader = new EPFFileReader(filePath);
				EPFImportTranslator importTranslator = new EPFImportTranslator(fileReader);
				EPFDbWriter dbWriter = EPFDbWriterFactory.getDbWriter(dbConfig);
				EPFImportTask importTask = new EPFImportTask(importTranslator, dbWriter);
				importThreads.add((Future<Runnable>) service.submit(importTask));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (EPFFileFormatException e) {
				e.printStackTrace();
			} catch (EPFDbException e) {
				e.printStackTrace();
			}

		}
	}
	
	public boolean isRunning() {
		for (Future<Runnable> f : importThreads){
			if (!f.isDone()) {
				return true;
			}
		}
		return false;
	}

	public List<String> getFileList() {
		return fileList;
	}

	public void setFileList(List<String> fileList) {
		this.fileList = fileList;
	}
}

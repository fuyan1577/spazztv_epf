/**
 * 
 */
package com.spazztv.epf;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.spazztv.epf.dao.EPFDbConfig;
import com.spazztv.epf.dao.EPFDbException;
import com.spazztv.epf.dao.EPFDbWriter;
import com.spazztv.epf.dao.EPFDbWriterFactory;

/**
 * This is the main work manager class of the EPFImport process that queues all
 * EPFImportTasks.
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFImportManager {
	private ExecutorService threadPoolService;
	private List<String> fileList;
	private String whiteListRegex;
	private String blackListRegex;
	private List<Future<Runnable>> importThreads;

	public EPFImportManager(EPFConfig config, EPFDbConfig dbConfig) {
		threadPoolService = Executors
				.newFixedThreadPool(config.getMaxThreads());

		whiteListRegex = createRegexPattern(config.getWhiteList());
		blackListRegex = createRegexPattern(config.getBlackList());
		loadImportFileList(config.getDirectoryPaths());
		loadImportThreads(config, dbConfig);
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

	private void loadImportFileList(List<String> directoryPaths) {
		fileList = new ArrayList<String>();

		for (String directoryPath : directoryPaths) {
			File dir = new File(directoryPath);

			FileFilter skipDirectoriesFilter = new FileFilter() {
				public boolean accept(File file) {
					return !file.isDirectory();
				}
			};

			File[] dirFile = dir.listFiles(skipDirectoriesFilter);
			if (dirFile != null) {
				for (int i = 0; i < dirFile.length; i++) {
					// Get filename of file or directory
					if (isValidImportFile(dirFile[i].getName())) {
						fileList.add(dirFile[i].getPath());
					}
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
	private boolean isValidImportFile(String importFile) {
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
	private void loadImportThreads(EPFConfig config, EPFDbConfig dbConfig) {
		importThreads = new ArrayList<Future<Runnable>>();
		for (String filePath : fileList) {
			try {
				EPFDbWriter dbWriter = EPFDbWriterFactory.getDbWriter(dbConfig);
				EPFImportTask importTask = new EPFImportTask(filePath,
						config.getRecordSeparator(), dbWriter);
				importThreads.add((Future<Runnable>) threadPoolService
						.submit(importTask));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (EPFFileFormatException e) {
				e.printStackTrace();
			} catch (EPFDbException e) {
				e.printStackTrace();
			}
		}
	}

	public boolean isRunning() throws EPFDbException {
		for (Future<Runnable> f : importThreads) {
			if (!f.isDone()) {
				return true;
			}
		}
		threadPoolService.shutdown();
		EPFDbWriterFactory.closeFactory();
		return false;
	}

	/**
	 * FileList getter for Job Logger Advice.
	 * 
	 * @return the fileList
	 */
	public List<String> getFileList() {
		return fileList;
	}
}

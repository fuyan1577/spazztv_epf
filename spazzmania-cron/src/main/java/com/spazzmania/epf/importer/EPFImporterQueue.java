package com.spazzmania.epf.importer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class EPFImporterQueue {

	public static String IMPORT_QUEUE = "import_queue";
	public static String IMPORTED_FILES = "imported_files";
	public static String FAILED_FILES = "failed_files";

	private static EPFImporterQueue importerQueue;

	private boolean persist = false;
	private String snapshotFile;

	private List<String> importQueue;
	private List<String> failedFiles;
	private List<String> importedFiles;

	private EPFImporterQueue() {
		importQueue = new ArrayList<String>();
		failedFiles = new ArrayList<String>();
		importedFiles = new ArrayList<String>();
	}

	public static EPFImporterQueue getInstance() {
		if (importerQueue == null) {
			importerQueue = new EPFImporterQueue();
		}
		return importerQueue;
	}

	@SuppressWarnings("unchecked")
	public synchronized void updateSnapshot() {
		if (!persist) {
			return;
		}
		FileOutputStream fos;
		OutputStreamWriter out;
		try {
			JSONObject snapshot = new JSONObject();
			snapshot.put(IMPORT_QUEUE, importQueue);
			snapshot.put(IMPORTED_FILES, importedFiles);
			snapshot.put(FAILED_FILES, failedFiles);
			fos = new FileOutputStream(snapshotFile);
			out = new OutputStreamWriter(fos, "UTF-8");
			out.write(snapshot.toJSONString());
			out.flush();
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	public synchronized void loadSnapshot() throws EPFImporterException {
		persist = true;
		InputStream fis;
		Reader in;
		JSONParser parser = new JSONParser();
		try {
			fis = new FileInputStream(snapshotFile);
			in = new InputStreamReader(fis);
			JSONObject snapshot = (JSONObject) parser.parse(in);
			importQueue = (List<String>)snapshot.get(IMPORT_QUEUE);
			failedFiles = (List<String>)snapshot.get(IMPORTED_FILES);
			importedFiles = (List<String>)snapshot.get(FAILED_FILES);
			in.close();
		} catch (Exception e) {
			throw new EPFImporterException(e.getMessage());
		}
	}

	public synchronized void add(String fileName) {
		importQueue.add(fileName);
		updateSnapshot();
	}

	public synchronized void setFailed(String fileName) {
		importQueue.remove(fileName);
		failedFiles.add(fileName);
	}

	public synchronized void setCompleted(String fileName) {
		importQueue.remove(fileName);
		importedFiles.add(fileName);
	}

	public synchronized void setSnapshotFile(String snapshotFile) {
		this.snapshotFile = snapshotFile;
		if (this.snapshotFile != null) {
			persist = true;
		} else {
			persist = false;
		}
	}

	public synchronized List<String> getImportQueue() {
		return importQueue;
	}

	public synchronized List<String> getFailedFiles() {
		return failedFiles;
	}

	public synchronized List<String> getImportedFiles() {
		return importedFiles;
	}
}

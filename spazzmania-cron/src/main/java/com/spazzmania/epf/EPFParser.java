/**
 * 
 */
package com.spazzmania.epf;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Thomas Billingsley
 * 
 */
public class EPFParser {
	private String filePath;
	private RandomAccessFile eFile;
	private long totalRecords = 0L;
	private String recordDelim = "\\x01";
	private String fieldDelim = "\\x02";

	private List<String> columnNames;
	private Map<String, String> dataTypes;
	private ExportMode exportMode;

	private long lastRecordNum = 0;

	private boolean endOfFile = false;

	public static String COMMENT_CHAR = "#";
	public static String PRIMARY_KEY_TAG = "primaryKey:";
	public static String DATA_TYPES_TAG = "dbTypes:";
	public static String EXPORT_MODE_TAG = "exportMode:";
	public static String RECORDS_WRITTEN_TAG = "recordsWritten:";

	public enum ExportMode {
		FULL, INCREMENTAL
	}

	// (self, filePath, typeMap={"CLOB":"LONGTEXT"}, recordDelim='\x02\n',
	// fieldDelim='\x01')
	public EPFParser(String filePath, Map<String, String> typeMap,
			String recordDelim, String fieldDelim) {
		this.filePath = filePath;
		this.recordDelim = recordDelim;
		this.fieldDelim = fieldDelim;

		typeMap = new HashMap<String, String>();
		typeMap.put("CLOB", "LONGTEXT");
		init();
	}

	public List<String> nextRecord() {
		List<String> rec = splitRow(nextRowString(true), fieldDelim);
		return rec;
	}

	public List<List<String>> nextRecords(int records) {
		List<List<String>> recList = new ArrayList<List<String>>();
		for (int i = 0; i < records; i++) {
			if (endOfFile) {
				break;
			}
			recList.add(nextRecord());
		}

		return recList;
	}

	public void seekToRecord(int recordNum) {
		try {
			eFile.seek(0L);
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (recordNum <= 0) {
			return;
		}
		
		for (int i = 0; i < recordNum; i++) {
			advanceToNextRecord();
			if (endOfFile) {
				break;
			}
		}
	}

	private void advanceToNextRecord() {
		try {
			while (true) {
				String nextRow = eFile.readLine();
				lastRecordNum++;
				if (!nextRow.startsWith(COMMENT_CHAR)) {
					break;
				}
			}
		} catch (IOException e) {
		}
	}

	private String nextRowString(boolean skipComments) {
		String nextRow = null;
		try {
			while (true) {
				nextRow = eFile.readLine();
				lastRecordNum++;
				if ((!skipComments) || (!nextRow.startsWith(COMMENT_CHAR))) {
					break;
				}
			}
		} catch (IOException e) {
			endOfFile = true;
		}
		return nextRow;
	}

	private void init() {
		openInputFile();
		loadTotalRecords();
		loadRecordDefinitions();
	}

	private void loadTotalRecords() {
		try {
			eFile.readChar();
			eFile.seek(eFile.length() - 40);
			String buff = "";
			for (int i = 0; i < 40; i++) {
				buff = buff + eFile.readChar();
			}
			eFile.close();

			buff = buff.replaceAll(".+" + RECORDS_WRITTEN_TAG, "");
			buff = buff.replaceAll(String.valueOf(recordDelim), "");
			totalRecords = Long.parseLong(buff);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void openInputFile() {
		File file = new File(filePath);
		try {
			eFile = new RandomAccessFile(file, "r");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			try {
				eFile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void loadRecordDefinitions() {
		try {
			eFile.seek(0L);
			parseColumnNames(eFile.readLine());

			for (int i = 0; i < 6; i++) {
				String nextRow = eFile.readLine();
				if (nextRow.startsWith(PRIMARY_KEY_TAG)) {
					parsePrimaryKey(nextRow);
				} else if (nextRow.startsWith(DATA_TYPES_TAG)) {
					parseAndSetDataTypes(nextRow);
				} else if (nextRow.startsWith(EXPORT_MODE_TAG)) {
					parseExportMode(nextRow);
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public List<String> splitRow(String row, String requiredPrefix) {
		String r = row.split(recordDelim)[0];
		if (requiredPrefix != null) {
			if (!r.contains(requiredPrefix)) {
				throw new RuntimeException("Row from " + filePath
						+ "does not have requiredPrefix: " + requiredPrefix);
			}
			r = row.replaceFirst(".*" + requiredPrefix, "");
		}

		return Arrays.asList(r.split(fieldDelim));
	}

	public void parseColumnNames(String row) {
		columnNames = splitRow(row, COMMENT_CHAR);
	}

	public void parsePrimaryKey(String row) {
	}

	public void parseAndSetDataTypes(String row) {
		List<String> dTypes = splitRow(row, COMMENT_CHAR + DATA_TYPES_TAG);

		String[] dts = (String[]) dTypes.toArray();
		String[] cts = (String[]) columnNames.toArray();

		for (int i = 0; i < (cts.length < dts.length ? cts.length : dts.length); i++) {
			dataTypes.put(cts[i], dts[i]);
		}
	}

	public void parseExportMode(String row) {
		List<String> dTypes = splitRow(row, COMMENT_CHAR + EXPORT_MODE_TAG);
		String eMode = dTypes.get(0);
		exportMode = ExportMode.FULL;
		if (eMode == ExportMode.INCREMENTAL.toString()) {
			exportMode = ExportMode.INCREMENTAL;
		}
	}
}

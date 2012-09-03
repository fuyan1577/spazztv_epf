/**
 * 
 */
package com.spazzmania.epf.ingester;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * EPFParser object is a java port of EPFParser.py Python script.
 * <p>
 * This object reads data from an EPF file parsing the column names, data types,
 * and data and handing the parsed data back as lists to the calling program.
 * 
 * <p>
 * The parsing logic is designed to be carried out in the following sequence:
 * <ul>
 * <li/>Retrieve the total data records for the import file from the end of the
 * import file
 * <li/>Retrieve the column names, column data types and primary key fields from
 * the header records of the file
 * <li/>Import the data records (record by record or by groups of records)
 * </ul>
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFParser {
	private EPFFileReader eFile;
	private long totalRecords = 0L;
	private String recordDelim = "\\x01";
	private String fieldDelim = "\\x02";

	private List<String> columnNames;
	private List<String> dataTypes;
	private ExportMode exportMode;
	private List<String> primaryKey;

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

	// (eFile, recordDelim='\x02\n',
	// fieldDelim='\x01')
	/**
	 * EPFParser - create a new instance of this object for each file to be
	 * imported.
	 * 
	 * <p>
	 * The EPFFileReader object is assumed to be instantiated and ready to
	 * retrieve data.
	 * 
	 * <p>
	 * The record delimiter and field delimiter values are configurable in the
	 * EPFConfig.json and EPFFlatConfig.json.
	 * 
	 * @param eFile
	 *            - EPFFileReader for a given EPF Import File
	 * @param recordDelim
	 *            - <i>usually either '\x02' or '\n'</i>
	 * @param fieldDelim
	 *            - <i>usually either '\x01' or '\t'</i>
	 */
	public EPFParser(EPFFileReader eFile, String recordDelim, String fieldDelim) {
		this.eFile = eFile;
		this.recordDelim = recordDelim;
		this.fieldDelim = fieldDelim;

		init();
	}

	/**
	 * Return the total number of records expected. This is the number of
	 * records designated on the last line of the EPF Import file.
	 * 
	 * @return total records expected
	 */
	public long getTotalRecordsExpected() {
		return totalRecords;
	}

	/**
	 * Return the last data record number read. This value is set to Zero (0) on
	 * instantiation and is incremented each time nextRecord() or nextRecords()
	 * is called until the end of file is reached.
	 * 
	 * @return the number of the last data record read
	 */
	public long getLastRecordRead() {
		return lastRecordNum;
	}

	/**
	 * Return the columns and data types of the EPF Import file.
	 * 
	 * @return a LinkedHashMap of column names and their data types.
	 */
	public LinkedHashMap<String, String> getColumnAndTypes() {
		LinkedHashMap<String, String> columnsAndTypes = new LinkedHashMap<String, String>();

		String[] dts = (String[]) dataTypes.toArray();
		String[] cts = (String[]) columnNames.toArray();

		for (int i = 0; i < (cts.length < dts.length ? cts.length : dts.length); i++) {
			columnsAndTypes.put(cts[i], dts[i]);
		}

		return columnsAndTypes;
	}

	/**
	 * Return a list of one or more column names comprising the primary key.
	 * 
	 * @return the primary key fields
	 */
	public List<String> getPrimaryKey() {
		return primaryKey;
	}

	/**
	 * 
	 * @return
	 */
	public ExportMode getExportMode() {
		return exportMode;
	}

	/**
	 * Return the next data row from the import file as a List&ltString&gt of
	 * column values.
	 * 
	 * <p>
	 * If the end of the file is reached, a null row is returned.
	 * 
	 * @return - List&lt;String&gt of column values for the row
	 */
	public List<String> nextDataRecord() {
		List<String> rec = splitRow(nextDataRowString(), fieldDelim);
		return rec;
	}

	/**
	 * Returns the next <i>n</i> data records from the import file as a List of
	 * List&ltString&gt; values.
	 * 
	 * <p>
	 * This method wraps many calls to nextRecord().
	 * 
	 * <p>
	 * If the end of the file is reached, only those records read before the end
	 * file are returned.
	 * 
	 * @param records
	 *            - The number of records to retrieve
	 * @return - the next group of records
	 */
	public List<List<String>> nextRecords(int records) {
		List<List<String>> recList = new ArrayList<List<String>>();
		for (int i = 0; i < records; i++) {
			if (endOfFile) {
				break;
			}
			recList.add(nextDataRecord());
		}

		return recList;
	}

	/**
	 * Read until the next row is found or the end of file is reached.
	 * <p>
	 * If the skipComments option is true, this reads and returns the next
	 * uncommented row of data.
	 * 
	 * @param skipComments
	 *            If true, skips all commented lines
	 * @return the next row of data or null on end of file
	 */
	private String nextDataRowString() {
		String nextRow = null;
		try {
			nextRow = eFile.readNextDataLine();
			lastRecordNum++;
		} catch (IOException e) {
			endOfFile = true;
		}
		return nextRow;
	}

	/**
	 * Initialize the import loading the total records and record definitions.
	 */
	private void init() {
		loadTotalRecords();
		loadRecordDefinitions();
	}

	/**
	 * Load the total records designated in the EPF Import file. The last row of
	 * data in the import file indicates the total records.
	 */
	private void loadTotalRecords() {
		try {
			String buff = eFile.readRecordsWrittenLine(RECORDS_WRITTEN_TAG);
			buff = buff.replaceAll(".+" + RECORDS_WRITTEN_TAG, "");
			buff = buff.replaceAll(String.valueOf(recordDelim), "");
			totalRecords = Long.parseLong(buff);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Read the parse the Column Names, Data Types, Primary Key Fields and
	 * Export Mode from the beginning of the import file.
	 * <p>
	 * Rewind setting the file pointer to the beginning of the import file.
	 */
	private void loadRecordDefinitions() {
		try {
			eFile.rewind();
			parseColumnNames(eFile.readNextHeaderLine());

			for (int i = 0; i < 6; i++) {
				String nextRow = eFile.readNextHeaderLine();
				if (nextRow.startsWith(PRIMARY_KEY_TAG)) {
					parsePrimaryKey(nextRow);
				} else if (nextRow.startsWith(DATA_TYPES_TAG)) {
					parseDataTypes(nextRow);
				} else if (nextRow.startsWith(EXPORT_MODE_TAG)) {
					parseExportMode(nextRow);
				}
			}
			eFile.rewind();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Split the input string into separate column. The row is split by the
	 * fieldDelim value designated on instantiation.
	 * <p>
	 * The requiredPrefix if provided is verified to be present and removed
	 * before the data is parsed and returned.
	 * 
	 * @param row
	 *            String of row data
	 * @param requiredPrefix
	 *            optional required prefix string
	 * @return parsed data list
	 */
	private List<String> splitRow(String row, String requiredPrefix) {
		String r = row.split(recordDelim)[0];
		if (requiredPrefix != null) {
			if (!r.contains(requiredPrefix)) {
				throw new RuntimeException("Row from " + eFile.getFilePath()
						+ "does not have requiredPrefix: " + requiredPrefix);
			}
			r = row.replaceFirst(".*" + requiredPrefix, "");
		}

		return Arrays.asList(r.split(fieldDelim));
	}

	/**
	 * Parses the column names from the provided row String and updates the
	 * columnNames property of this object.
	 * 
	 * @param row
	 */
	private void parseColumnNames(String row) {
		columnNames = splitRow(row, COMMENT_CHAR);
	}

	/**
	 * Parses the primary key column(s) from he provided row String and updates
	 * the primaryKey property of this object.
	 * 
	 * @param row
	 */
	private void parsePrimaryKey(String row) {
		primaryKey = splitRow(row, PRIMARY_KEY_TAG);
	}

	/**
	 * Parses the data type definitions from the provided row String and updates
	 * the dataTypes property of this object.
	 * 
	 * @param row
	 */
	private void parseDataTypes(String row) {
		dataTypes = splitRow(row, COMMENT_CHAR + DATA_TYPES_TAG);
	}

	/**
	 * Parses the export mode definition from the provided row String and
	 * updates the exportMode property of this object.
	 * 
	 * @param row
	 */
	private void parseExportMode(String row) {
		List<String> dTypes = splitRow(row, COMMENT_CHAR + EXPORT_MODE_TAG);
		String eMode = dTypes.get(0);
		exportMode = ExportMode.FULL;
		if (eMode == ExportMode.INCREMENTAL.toString()) {
			exportMode = ExportMode.INCREMENTAL;
		}
	}
}

package com.spazzmania.epf.ingester;

import java.io.IOException;
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
public class EPFImportTranslator {
	private EPFFileReader epfFileReader;
	private String recordDelim = DEFAULT_ROW_SEPARATOR;
	private String fieldDelim = DEFAULT_FIELD_SEPARATOR;

	private String tableName;
	private List<String> columnNames;
	private List<String> dataTypes;
	private EPFExportType exportType;
	private List<String> primaryKey;

	private long lastRecordNum = 0;

	public static String COMMENT_CHAR = "#";
	public static String PRIMARY_KEY_TAG = "primaryKey:";
	public static String DATA_TYPES_TAG = "dbTypes:";
	public static String EXPORT_MODE_TAG = "exportMode:";
	public static String DEFAULT_FIELD_SEPARATOR = "\\x01";
	public static String DEFAULT_ROW_SEPARATOR = "\\x02";
	public static String DEFAULT_FLAT_SEPARATOR = "\\t";

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
	 * @param epfFileReader
	 *            - EPFFileReader for a given EPF Import File
	 */
	public EPFImportTranslator(EPFFileReader epfFileReader)
			throws EPFFileFormatException {
		this.epfFileReader = epfFileReader;

		init();
	}

	/**
	 * Return the table name. Table names are based on the input file name.
	 * 
	 * @return
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * Return the total number of records expected. This is the number of
	 * records designated on the last line of the EPF Import file.
	 * 
	 * @return total data records
	 */
	public long getTotalDataRecords() {
		return epfFileReader.getRecordsWritten();
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
	public EPFExportType getExportType() {
		return exportType;
	}

	/**
	 * Returns true if there are more data record in the input EPFFileReader
	 * object
	 * 
	 * @return true if there are more data records
	 */
	public boolean hasNextRecord() {
		return epfFileReader.hasNextDataRecord();
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
	public String[] nextRecord() {
		// Data rows have no prefix...
		return splitRow(nextDataRowString(), null);
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
		if (!epfFileReader.hasNextDataRecord()) {
			return null;
		}
		String nextRow = null;
		try {
			nextRow = epfFileReader.nextDataLine();
			lastRecordNum++;
		} catch (IOException e) {
		}
		return nextRow;
	}

	/**
	 * Initialize the import loading the total records and record definitions.
	 */
	private void init() throws EPFFileFormatException {
		loadRecordDefinitions();
		loadTableName();
	}

	private void loadTableName() {
		// Start with the filename without the extension
		String wrkName = epfFileReader.getFilePath().replaceAll("\\\\", "/");
		wrkName = wrkName.replaceAll(".+/([^/]+)", "$1");
		if (wrkName.matches("^.+\\..*$")) {
			wrkName = wrkName.replaceAll("^(.+)\\..*$", "$1");
		}
		// Convert any hyphens (-) to underscores (_)
		wrkName = wrkName.replaceAll("-", "_");

		// Convert camel case abcDef to abc_Def
		while (wrkName.matches(".*[a-z][A-Z].*")) {
			wrkName = wrkName.replaceAll("([a-z])([A-Z])", "$1_$2");
		}
		// Convert all to lower case
		wrkName = wrkName.toLowerCase();

		tableName = wrkName;
	}

	/**
	 * Read the parse the Column Names, Data Types, Primary Key Fields and
	 * Export Mode from the beginning of the import file.
	 * <p>
	 * Rewind setting the file pointer to the beginning of the import file.
	 * 
	 * @throws EPFFileFormatException
	 */
	private void loadRecordDefinitions() throws EPFFileFormatException {
		try {
			epfFileReader.rewind();
			String nextRow = epfFileReader.nextHeaderLine();
			if (nextRow.matches(".+\\t.+")) {
				exportType = EPFExportType.FLAT;
			}
			parseColumnNames(nextRow);

			for (int i = 0; i < 6; i++) {
				nextRow = epfFileReader.nextHeaderLine();
				if (nextRow.startsWith(COMMENT_CHAR + PRIMARY_KEY_TAG)) {
					parsePrimaryKey(nextRow);
				} else if (nextRow.startsWith(COMMENT_CHAR + DATA_TYPES_TAG)) {
					parseDataTypes(nextRow);
				} else if (nextRow.startsWith(COMMENT_CHAR + EXPORT_MODE_TAG)) {
					parseExportType(nextRow);
				}
			}
			epfFileReader.rewind();

		} catch (IOException e) {
			e.printStackTrace();
		}
		if (exportType == null) {
			throw new EPFFileFormatException(String.format(
					"Invalid EPF File Format in file: %s",
					epfFileReader.getFilePath()));
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
	private String[] splitRow(String row, String requiredPrefix) {
		if (row == null) {
			return null;
		}
		String r = row.split(recordDelim)[0];
		if (requiredPrefix != null) {
			if (!r.contains(requiredPrefix)) {
				throw new RuntimeException("Row from "
						+ epfFileReader.getFilePath()
						+ "does not have requiredPrefix: " + requiredPrefix);
			}
			r = r.replaceFirst(".*" + requiredPrefix, "");
		}
		
		return r.split(fieldDelim);
	}

	/**
	 * Parses the column names from the provided row String and updates the
	 * columnNames property of this object.
	 * 
	 * @param row
	 */
	private void parseColumnNames(String row) {
		columnNames = Arrays.asList(splitRow(row, COMMENT_CHAR));
	}

	/**
	 * Parses the primary key column(s) from he provided row String and updates
	 * the primaryKey property of this object.
	 * 
	 * @param row
	 */
	private void parsePrimaryKey(String row) {
		primaryKey = Arrays.asList(splitRow(row, PRIMARY_KEY_TAG));
	}

	/**
	 * Parses the data type definitions from the provided row String and updates
	 * the dataTypes property of this object.
	 * 
	 * @param row
	 */
	private void parseDataTypes(String row) {
		dataTypes = Arrays.asList(splitRow(row, COMMENT_CHAR + DATA_TYPES_TAG));
	}

	/**
	 * Parses the export mode definition from the provided row String and
	 * updates the exportMode property of this object.
	 * 
	 * @param row
	 */
	private void parseExportType(String row) {
		String[] dTypes = splitRow(row, COMMENT_CHAR + EXPORT_MODE_TAG);
		String eMode = dTypes[0];
		exportType = EPFExportType.FULL;
		if (eMode == EPFExportType.INCREMENTAL.toString()) {
			exportType = EPFExportType.INCREMENTAL;
		}
	}
}

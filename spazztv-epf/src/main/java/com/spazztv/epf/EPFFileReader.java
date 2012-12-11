package com.spazztv.epf;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;

import org.apache.commons.lang3.StringEscapeUtils;

/**
 * This is an interface which consolidates the random and buffered access of the
 * EPF Import file.
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFFileReader {

	public static String COMMENT_PREFIX = "#";
	public static String RECORDS_WRITTEN_TAG = "recordsWritten:";

	private BufferedReader bFile;
	private String filePath;
	private long recordsWritten = 0;
	private long lastDataRecord = 0;
	private char recordSeparatorChar;

	public EPFFileReader(String filePath, String recordSeparator)
			throws FileNotFoundException, EPFFileFormatException {
		this.filePath = filePath;
		FileInputStream fstream = new FileInputStream(filePath);
		DataInputStream in = new DataInputStream(fstream);
		bFile = new BufferedReader(new InputStreamReader(in));
		recordSeparatorChar = StringEscapeUtils.unescapeXml(recordSeparator)
				.toCharArray()[0];
		loadRecordsWritten();
	}

	/**
	 * Read the next header line of data that begins with an
	 * EPFFileReader.COMMENT_PREFIX. The reads start from the current record
	 * position reading to the end of the line or the end of file and return the
	 * data as a String.
	 * <p>
	 * This method is intended to read the header rows of an EPF Import File. If
	 * the next row read does not match the header prefix, this routine does not
	 * read any further and returns a null.
	 * 
	 * @see java.io.RandomAccessFile#readLine()
	 * 
	 * @return String of data read
	 * @throws IOException
	 */
	public String nextHeaderLine() throws IOException {
		String rec;
		while (true) {
			try {
				rec = bFile.readLine();
				if (rec.startsWith(COMMENT_PREFIX)) {
					break;
				}
			} catch (IOException e) {
				rec = null;
				break;
			}
		}
		return rec;
	}

	/**
	 * Read the next line of data starting at the current record position
	 * reading to the end of the line or the end of file and return the data as
	 * a String.
	 * 
	 * <p>
	 * Note: Apple's EPF Relational Data Feed has records delimited by an
	 * &0002; followed by a \n. The configuration indicates that the delimiter
	 * is only the &0002; and no '\n'. To handle this, the logic of this
	 * method skips all '\n' characters when beginning a new record.
	 * 
	 * @see java.io.RandomAccessFile#readLine()
	 * 
	 * @return String of data read
	 * @throws IOException
	 */
	public String nextDataLine() {
		StringBuffer record = new StringBuffer();
		char[] nextChar = new char[1];
		// Read the next non-comment input record
		while (true) {
			try {
				// Read in the next block - read until separatorChar
				while (bFile.read(nextChar, 0, 1) > 0) {
					if (nextChar[0] == recordSeparatorChar) {
						break;
					}
					if (record.length() > 0) {
						record.append(nextChar[0]);
						// Record blocks may not begin with '\n'
					} else if (nextChar[0] != '\n') {
						record.append(nextChar[0]);
					}
				}
				if (record.toString().startsWith(COMMENT_PREFIX)) {
					// If this is a comment record, initialize it and read the
					// next record
					record.delete(0, record.length());
				} else {
					break;
				}
			} catch (IOException e) {
				return null;
			}
		}
		if (record.length() > 0) {
			lastDataRecord++;
		}
		return record.toString();
	}

	/**
	 * Read the total records line from the end of the EPF Import File.
	 * <p>
	 * The total records line is identified as the first row in the last 80
	 * bytes of the file which starts with the provided totalRecordsPrefix. If
	 * no record is found matching the pattern, a null string is returned.
	 * <p>
	 * A total records line example: <code>#recordsWritten:1220416</code>
	 * 
	 * @param totalRecordsPrefix
	 *            the prefix of the totalRecords line
	 * @return
	 * @throws EPFFileFormatException
	 */
	private String getRecordsWrittenLine() throws EPFFileFormatException {
		String row;
		recordsWritten = 0L;
		RandomAccessFile rFile = null;

		try {
			rFile = new RandomAccessFile(filePath, "r");
			rFile.seek(rFile.length() - 80);
		} catch (IOException e1) {
			throw new EPFFileFormatException(
					"Invalid EPF File - missing recordsWritten row: "
							+ filePath);
		}

		while (true) {
			try {
				row = rFile.readLine();
				if (row.startsWith(COMMENT_PREFIX)) {
					break;
				}
			} catch (IOException e) {
				row = null;
				break;
			}
		}

		return row;
	}

	private void loadRecordsWritten() throws EPFFileFormatException {
		recordsWritten = 0L;
		String row = getRecordsWrittenLine();
		recordsWritten = Long.decode(row.replaceAll("^" + COMMENT_PREFIX
				+ RECORDS_WRITTEN_TAG + "(\\d+).+", "$1"));
	}

	public long getRecordsWritten() {
		return recordsWritten;
	}

	public long getLastDataRecord() {
		return lastDataRecord;
	}

	/**
	 * Rewind the file pointer to the beginning of the file. This is intended to
	 * be used immediately after reading all the header rows.
	 */
	public void rewind() throws FileNotFoundException {
		if (bFile != null) {
			try {
				bFile.close();
			} catch (IOException e) {
				// Ignore - we're about to open it again
			}
		}
		FileInputStream fstream = new FileInputStream(filePath);
		DataInputStream in = new DataInputStream(fstream);
		bFile = new BufferedReader(new InputStreamReader(in));
		lastDataRecord = 0;
	}

	public boolean hasNextDataRecord() {
		return lastDataRecord < recordsWritten;
	}

	/**
	 * Returns the length or <i>size</i> of the file.
	 * 
	 * @see java.io.RandomAccessFile#length()
	 * 
	 * @return the length of this file, measured in bytes.
	 * @throws IOException
	 */
	public long length() throws IOException {
		RandomAccessFile rFile = new RandomAccessFile(filePath, "r");
		long len = rFile.length();
		rFile.close();
		return len;
	}

	/**
	 * Closes the input file.
	 * 
	 * @see java.io.RandomAccessFile#close()
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException {
		bFile.close();
	}

	/**
	 * Return the filePath provided on instantiation
	 * 
	 * @return filePath of the input file
	 */
	public String getFilePath() {
		return filePath;
	}
}

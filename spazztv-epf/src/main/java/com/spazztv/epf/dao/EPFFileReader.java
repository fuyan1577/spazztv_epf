/**
 * 
 */
package com.spazztv.epf.dao;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringEscapeUtils;

import com.spazztv.epf.EPFFileFormatException;

/**
 * EPFFileReader interface.
 * <p>
 * This is the basic interface for loading a specified EPF import file.
 * <p>
 * Initial implementations include <i>SimpleEPFFileReader</i> and
 * <i>FilteredGamesEPFFileReader</i>
 * 
 * @author Thomas Billingsley
 * 
 */
public abstract class EPFFileReader {

	private String filePath;
	private char fieldSeparatorChar;
	private char recordSeparatorChar;

	public EPFFileReader(String filePath, String fieldSeparator,
			String recordSeparator) throws IOException, EPFFileFormatException {
		this.filePath = filePath;
		recordSeparatorChar = StringEscapeUtils.unescapeXml(recordSeparator)
				.toCharArray()[0];
		fieldSeparatorChar = StringEscapeUtils.unescapeXml(fieldSeparator)
				.toCharArray()[0];
	}

	/**
	 * Return the recordWritten value as designated on the last line of the EPF
	 * table file
	 * 
	 * @return
	 */
	public abstract long getRecordsWritten();

	/**
	 * Read the next data record starting at the current record position reading
	 * to the next <i>recordSeparator</i> or the end of file and return the data
	 * as a String.
	 * 
	 * <p>
	 * Note: Apple's EPF Relational Data Feed has records delimited by an &0002;
	 * followed by a \n. The configuration indicates that the delimiter is only
	 * the &0002; and no '\n'. To handle this, the logic of this method skips
	 * all '\n' characters when beginning a new record.
	 * 
	 * @see java.io.RandomAccessFile#readLine()
	 * 
	 * @return String of data read
	 */
	public abstract List<String> nextDataRecord();

	/**
	 * Return the next header record as List<String> or return null upon reading
	 * reading a non-commented record.
	 * <p>
	 * Read the next header record of data that begins with an
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
	 */
	public abstract List<String> nextHeaderRecord();

	/**
	 * Rewind the file pointer to the beginning of the file.
	 */
	public abstract void rewind() throws IOException;

	/**
	 * Returns true if there are more data records or returns false when the end
	 * of file is reached.
	 * 
	 * @return true if at least 1 more data record is available to be read
	 */
	public abstract boolean hasNextDataRecord();

	/**
	 * Closes the input file.
	 * 
	 * @see java.io.RandomAccessFile#close()
	 * 
	 * @throws IOException
	 */
	public abstract void close() throws IOException;

	/**
	 * Return the file path provided on instantiation
	 * 
	 * @return
	 */
	public String getFilePath() {
		return filePath;
	}

	/**
	 * Return the fieldSeparatorChar provided on instantiation
	 * 
	 * @return
	 */
	public char getFieldSeparatorChar() {
		return fieldSeparatorChar;
	}

	/**
	 * Return the recordSeparatorChar provided on instantiation
	 * 
	 * @return
	 */
	public char getRecordSeparatorChar() {
		return recordSeparatorChar;
	}
}

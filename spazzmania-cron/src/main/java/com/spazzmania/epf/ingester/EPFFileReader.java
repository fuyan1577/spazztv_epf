package com.spazzmania.epf.ingester;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * This is an interface which consolidates the random and buffered access of the EPF
 * Import file.
 * 
 * @author Thomas Billingsley
 * 
 */
public interface EPFFileReader {

	public static String COMMENT_PREFIX = "#";

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
	public String readNextHeaderLine() throws IOException;

	/**
	 * Read the next line of data starting at the current record position
	 * reading to the end of the line or the end of file and return the data as
	 * a String.
	 * 
	 * @see java.io.RandomAccessFile#readLine()
	 * 
	 * @return String of data read
	 * @throws IOException
	 */
	public String readNextDataLine() throws IOException;

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
	 * @throws IOException
	 */
	public String readRecordsWrittenLine(String totalRecordsPrefix)
			throws IOException;

	/**
	 * Rewind the file pointer to the beginning of the file. This is intended to
	 * be used immediately after reading all the header rows.
	 */
	public void rewind() throws FileNotFoundException;

	/**
	 * Returns the length or <i>size</i> of the file.
	 * 
	 * @see java.io.RandomAccessFile#length()
	 * 
	 * @return the length of this file, measured in bytes.
	 * @throws IOException
	 */
	public long length() throws IOException;

	/**
	 * Closes the input file.
	 * 
	 * @see java.io.RandomAccessFile#close()
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException;

	/**
	 * Return the filePath provided on instantiation
	 * 
	 * @return filePath of the input file
	 */
	public String getFilePath();

}

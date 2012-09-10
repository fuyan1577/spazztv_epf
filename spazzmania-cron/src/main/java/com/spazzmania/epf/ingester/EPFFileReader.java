package com.spazzmania.epf.ingester;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;

/**
 * This is an interface which consolidates the random and buffered access of the EPF
 * Import file.
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFFileReader {

	public static String COMMENT_PREFIX = "#";

	BufferedReader bFile;
	String filePath;

	public EPFFileReader(String filePath) throws FileNotFoundException {
		this.filePath = filePath;
		FileInputStream fstream = new FileInputStream(filePath);
		DataInputStream in = new DataInputStream(fstream);
		bFile = new BufferedReader(new InputStreamReader(in));
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
	public String readNextHeaderLine() throws IOException {
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
	 * @see java.io.RandomAccessFile#readLine()
	 * 
	 * @return String of data read
	 * @throws IOException
	 */
	public String readNextDataLine() throws IOException {
		String rec;
		while (true) {
			try {
				rec = bFile.readLine();
				if (!rec.startsWith(COMMENT_PREFIX)) {
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
			throws IOException {
		String rec;

		RandomAccessFile rFile = new RandomAccessFile(filePath, "r");
		rFile.seek(rFile.length() - 80);

		while (true) {
			try {
				rec = rFile.readLine();
				if (rec.startsWith(totalRecordsPrefix)) {
					break;
				}
			} catch (IOException e) {
				rec = null;
				break;
			}
		}
		rFile.close();
		return rec;
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
				//Ignore - we're about to open it again
			}
		}
		FileInputStream fstream = new FileInputStream(filePath);
		DataInputStream in = new DataInputStream(fstream);
		bFile = new BufferedReader(new InputStreamReader(in));
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

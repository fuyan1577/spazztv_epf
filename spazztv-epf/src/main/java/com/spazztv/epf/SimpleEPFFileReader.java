package com.spazztv.epf;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * This is an interface which consolidates the random and buffered access of the
 * EPF Import file.
 * 
 * @author Thomas Billingsley
 * 
 */
public class SimpleEPFFileReader extends EPFFileReader {

	public static String COMMENT_PREFIX = "#";
	public static String RECORDS_WRITTEN_TAG = "recordsWritten:";
	public static String EPF_CHARACTER_ENCODING = "UTF-8";

	private BufferedReader bFile;
	private long recordsWritten = 0;
	private long lastDataRecord = 0;
	private boolean endOfFile = false;

	/**
	 * SimpleEPFFileReader instantiation
	 * @param filePath - path and name of the EPF file to import
	 * @param fieldSeparator - field separator escaped format
	 * @param recordSeparator - record separator escaped format
	 * @throws IOException
	 * @throws EPFFileFormatException
	 */
	public SimpleEPFFileReader(String filePath, String fieldSeparator,
			String recordSeparator) throws IOException, EPFFileFormatException {
		super(filePath,fieldSeparator,recordSeparator);
		openInputFile();
		loadRecordsWritten();
	}
	
	@Override
	public List<String> nextHeaderRecord() {
		List<String> record;
		while (true) {
			record = nextRecord();
			if (record == null) {
				break;
			}
			if (record.size() < 1) {
				break;
			}
			if (record.get(0).startsWith(COMMENT_PREFIX)) {
				break;
			} else {
				//We've read beyond the header records - return a null
				return null;
			}
		}
		return record;
	}

	private List<String> nextRecord() {
		StringBuffer fieldBuffer = new StringBuffer();
		List<String> record = new ArrayList<String>();
		char[] nextChar = new char[1];
		try {
			// Read in the next block - read until separatorChar
			while (!endOfFile) {
				if (bFile.read(nextChar, 0, 1) < 0) {
					endOfFile = true;
				}
				if (nextChar[0] == getFieldSeparatorChar()) {
					record.add(fieldBuffer.toString());
					fieldBuffer.setLength(0);
				} else if (nextChar[0] == getRecordSeparatorChar()) {
					record.add(fieldBuffer.toString());
					break;
				} else if ((fieldBuffer.length() > 0) || (nextChar[0] != '\n')) {
					fieldBuffer.append(nextChar[0]);
				}
			}
		} catch (IOException e) {
			return null;
		}
		return record;
	}

	@Override
	public List<String> nextDataRecord() {
		List<String> record;
		// Read the next non-comment input record
		while (true) {
			// Read in the next block - read until separatorChar
			if ((record = nextRecord()) == null) {
				break;
			}
			if (record.size() < 1) {
				break;
			}
			if (!record.get(0).startsWith(COMMENT_PREFIX)) {
				break;
			}
		}
		if (endOfFile) {
			return null;
		}
		if (record.size() > 0) {
			lastDataRecord++;
		}
		return record;
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
			rFile = new RandomAccessFile(getFilePath(), "r");
			rFile.seek(rFile.length() - 80);
		} catch (IOException e1) {
			throw new EPFFileFormatException(
					"Invalid EPF File - missing recordsWritten row: "
							+ getFilePath());
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

	@Override
	public void rewind() throws IOException {
		if (bFile != null) {
			try {
				bFile.close();
			} catch (IOException e) {
				// Ignore - we're about to open it again
			}
		}
		openInputFile();
		lastDataRecord = 0;
	}

	private void openInputFile() throws IOException {
		FileInputStream fstream = new FileInputStream(getFilePath());
		DataInputStream in = new DataInputStream(fstream);
		try {
			bFile = new BufferedReader(new InputStreamReader(in,
					EPF_CHARACTER_ENCODING));
		} catch (UnsupportedEncodingException e) {
			throw new IOException(e.getMessage());
		}
	}

	@Override
	public boolean hasNextDataRecord() {
		if (!endOfFile) {
			return lastDataRecord < recordsWritten;
		}
		return false;
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
		RandomAccessFile rFile = new RandomAccessFile(getFilePath(), "r");
		long len = rFile.length();
		rFile.close();
		return len;
	}

	@Override
	public void close() throws IOException {
		bFile.close();
	}
}

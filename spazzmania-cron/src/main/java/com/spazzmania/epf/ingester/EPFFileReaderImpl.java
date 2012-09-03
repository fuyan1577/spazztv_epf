/**
 * 
 */
package com.spazzmania.epf.ingester;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;

/**
 * @author Thomas Billingsley
 * 
 */
public class EPFFileReaderImpl implements EPFFileReader {

	BufferedReader bFile;
	String filePath;

	public EPFFileReaderImpl(String filePath) throws FileNotFoundException {
		this.filePath = filePath;
		FileInputStream fstream = new FileInputStream(filePath);
		DataInputStream in = new DataInputStream(fstream);
		bFile = new BufferedReader(new InputStreamReader(in));
	}

	/* (non-Javadoc)
	 * @see com.spazzmania.epf.ingester.EPFFileReader#readNextHeaderLine()
	 */
	@Override
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

	/* (non-Javadoc)
	 * @see com.spazzmania.epf.ingester.EPFFileReader#readNextDataLine()
	 */
	@Override
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.spazzmania.epf.ingester.EPFFileReader#readTotalRecordsExported()
	 */
	@Override
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

	/* (non-Javadoc)
	 * @see com.spazzmania.epf.ingester.EPFFileReader#rewind()
	 */
	@Override
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.spazzmania.epf.ingester.EPFFileReader#length()
	 */
	@Override
	public long length() throws IOException {
		RandomAccessFile rFile = new RandomAccessFile(filePath, "r");
		long len = rFile.length();
		rFile.close();
		return len;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.spazzmania.epf.ingester.EPFFileReader#close()
	 */
	@Override
	public void close() throws IOException {
		bFile.close();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.spazzmania.epf.ingester.EPFFileReader#getFilePath()
	 */
	@Override
	public String getFilePath() {
		return filePath;
	}
	
}

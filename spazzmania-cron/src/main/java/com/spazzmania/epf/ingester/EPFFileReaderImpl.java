/**
 * 
 */
package com.spazzmania.epf.ingester;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Thomas Billingsley
 *
 */
public class EPFFileReaderImpl implements EPFFileReader {
	
	RandomAccessFile rFile;

	/* (non-Javadoc)
	 * @see com.spazzmania.epf.ingester.EPFFileReader#openFile(java.lang.String)
	 */
	@Override
	public void openFile(String filePath) throws FileNotFoundException {
		rFile = new RandomAccessFile(filePath, "r");
	}

	/* (non-Javadoc)
	 * @see com.spazzmania.epf.ingester.EPFFileReader#seek(long)
	 */
	@Override
	public void seek(long pos) throws IOException {
		rFile.seek(pos);
	}

	/* (non-Javadoc)
	 * @see com.spazzmania.epf.ingester.EPFFileReader#readLine()
	 */
	@Override
	public String readLine() throws IOException {
		return rFile.readLine();
	}

	/* (non-Javadoc)
	 * @see com.spazzmania.epf.ingester.EPFFileReader#readChar()
	 */
	@Override
	public char readChar() throws IOException {
		return rFile.readChar();
	}

	/* (non-Javadoc)
	 * @see com.spazzmania.epf.ingester.EPFFileReader#length()
	 */
	@Override
	public long length() throws IOException {
		return rFile.length();
	}

	/* (non-Javadoc)
	 * @see com.spazzmania.epf.ingester.EPFFileReader#close()
	 */
	@Override
	public void close() throws IOException {
		rFile.close();
	}

}

package com.spazzmania.epf.ingester;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * This is an interface which wraps the RandomAccessFile and was created for the
 * sole purpose of creating mocks for JUnit Testing.
 * 
 * @author Thomas Billingsley
 * 
 */
public interface EPFFileReader {

	public void openFile(String filePath) throws FileNotFoundException;

	public void seek(long pos) throws IOException;

	public String readLine() throws IOException;

	public char readChar() throws IOException;

	public long length() throws IOException;

	public void close() throws IOException;

}

package com.spazztv.epf.dao;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.spazztv.epf.adapter.SimpleEPFFileReader;

/**
 * This is a test to randomly access the EPF application table while still being
 * able to read UTF8 characters.
 * <p>The main thing learned from this excercise is that EPF Application files can be retrieved
 * using the RandomAccessFile object, reading specific blocks of byte[] arrays and then encoding them into a
 * UTF-8 string using <b>new String(byteArray, "UTF-8")</b>.
 * <p>To save the original byte offsets within the file of the location of application records, use the BufferedReader to read 1 char at a time
 * but keeping track of the byte offset instead of the char offset. The length of a UTF-8 char in bytes is determined 
 * using the String object, <b>String.valueOf(nextChar).getBytes("UTF-8").length</b>.
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFRandomAccessUTF8Test {

	public static String UTF8_ENCODING = "UTF-8";

	private String filename = "testdata/epf_files/application_220";
	private BufferedReader bFile;
	private boolean endOfFile;
	private long bOffset;
	private List<ApplicationEntry> applicationEntries;
	private char fieldSeparatorChar = '\u0001';
	private char recordSeparatorChar = '\u0002';

	private class ApplicationEntry {
		public ApplicationEntry(String applicationId, long offset, int length) {
			this.applicationId = applicationId;
			this.offset = offset;
			this.length = length;
		}

		public String applicationId;
		public long offset;
		public int length;
	}

	private class EpfRecord {
		public List<String> data;
		public long offset;
		public int length;
	}

	@Before
	public void setUp() throws IOException {
		bFile = openApplicationFile();
		loadFirst100Applications();
		bFile.close();
	}
	
	public BufferedReader openApplicationFile() throws IOException {
		FileInputStream fstream = new FileInputStream(filename);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader reader;
		try {
			reader = new BufferedReader(new InputStreamReader(in, UTF8_ENCODING));
		} catch (UnsupportedEncodingException e) {
			throw new IOException(e.getMessage());
		}
		
		return reader;
	}

	public void loadFirst100Applications() {
		applicationEntries = new ArrayList<ApplicationEntry>();
		boolean readNext = true;
		int appCount = 0;
		while (readNext) {
			EpfRecord record = nextRecord();
			if (endOfFile) {
				break;
			}
			if (!record.data.get(0).startsWith(
					SimpleEPFFileReader.COMMENT_PREFIX)) {
				applicationEntries.add(new ApplicationEntry(record.data.get(1),
						record.offset, record.length));
				if (record.data.get(1).equals("550416064")) {
					appCount += 0;
				}
				appCount++;
				if (appCount >= 100) {
					readNext = false;
				}
			}
		}
	}

	private EpfRecord nextRecord() {
		StringBuffer fieldBuffer = new StringBuffer();
		char[] nextChar = new char[1];
		long bRecordOffset = bOffset; 
		int len = 0;
		List<String> record = new ArrayList<String>();
		try {
			// Read in the next block - read until separatorChar
			while (!endOfFile) {
				if (bFile.read(nextChar, 0, 1) < 0) {
					endOfFile = true;
				} else {
					len = String.valueOf(nextChar[0]).getBytes("UTF-8").length;
					if (len != 1) {
						len = len + 0;
					}
					bOffset += len;
				}
				if (nextChar[0] == fieldSeparatorChar) {
					record.add(fieldBuffer.toString());
					fieldBuffer.setLength(0);
				} else if (nextChar[0] == recordSeparatorChar) {
					record.add(fieldBuffer.toString());
					break;
				} else if ((fieldBuffer.length() > 0) || (nextChar[0] != '\n')) {
					fieldBuffer.append(nextChar[0]);
				}
			}
		} catch (IOException e) {
			return null;
		}
		EpfRecord rec = new EpfRecord();
		rec.data = record;
		rec.offset = bRecordOffset;
		rec.length = (int)(bOffset - bRecordOffset);
		return rec;
	}
	
	public List<String> parseFields(char[] recordChar) {
		StringBuffer fieldBuffer = new StringBuffer();
		List<String> fieldList = new ArrayList<String>();
		for (int i = 0; i < recordChar.length; i++) {
			if (recordChar[i] == fieldSeparatorChar) {
				fieldList.add(fieldBuffer.toString());
				fieldBuffer.setLength(0);
			} else if (recordChar[i] == recordSeparatorChar) {
				fieldList.add(fieldBuffer.toString());
				break;
			} else if ((fieldBuffer.length() > 0) || (recordChar[i] != '\n')) {
				fieldBuffer.append(recordChar[i]);
			}
		}
		return fieldList;
	}
	
	@Test
	public void testRandomBufferedReads() throws IOException {
		int maxTests = 10;
		int curTest = 0;

		RandomAccessFile rFile = new RandomAccessFile(filename,"r");
		endOfFile = false;

		for (int i = 0; i < 100; i++) {
			ApplicationEntry appEntry = applicationEntries.get(i);
			bOffset = appEntry.offset;
			
			byte[] buf = new byte[appEntry.length];
			rFile.seek(appEntry.offset);
			rFile.read(buf,0,appEntry.length);
			List<String> rec = parseFields(new String(buf,"UTF-8").toCharArray());
			if (endOfFile) {
				break;
			}
			Assert.assertTrue(rec.get(1).equals(appEntry.applicationId));
			if (curTest++ >= maxTests) {
				break;
			}
		}
		
		bFile.close();
	}
}

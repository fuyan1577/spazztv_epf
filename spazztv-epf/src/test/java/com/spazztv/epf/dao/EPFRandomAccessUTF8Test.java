/**
 * 
 */
package com.spazztv.epf.dao;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFRandomAccessUTF8Test {

	public static String UTF8_ENCODING = "UTF-8";

	private String filename = "testdata/epf_files/application";
	private BufferedReader bFile;
	private boolean endOfFile;
	private long bOffset;
	private List<ApplicationEntry> applicationEntries;
	private char fieldSeparatorChar = "&#0001".toCharArray()[0];
	private char recordSeparatorChar = "&#0002".toCharArray()[0];

	private class ApplicationEntry {
		public ApplicationEntry(String applicationId, long offset, long length) {
			this.applicationId = applicationId;
			this.offset = offset;
			this.length = length;
		}

		public String applicationId;
		public long offset;
		public long length;
	}

	private class EpfRecord {
		public List<String> data;
		public long offset;
		public long length;
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
			if (!record.data.get(0).startsWith(
					SimpleEPFFileReader.COMMENT_PREFIX)) {
				applicationEntries.add(new ApplicationEntry(record.data.get(1),
						record.offset, record.length));
				appCount++;
				if ((appCount >= 100) || (endOfFile)) {
					readNext = false;
				}
			}
		}
	}

	private EpfRecord nextRecord() {
		StringBuffer fieldBuffer = new StringBuffer();
		char[] nextChar = new char[1];
		long bRecordOffset = bOffset;
		List<String> record = new ArrayList<String>();
		try {
			// Read in the next block - read until separatorChar
			while (!endOfFile) {
				if (bFile.read(nextChar, 0, 1) < 0) {
					endOfFile = true;
				} else {
					bOffset++;
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
		rec.length = bOffset - bRecordOffset;
		return rec;
	}
	
	@Test
	public void testRandomBufferedReads() throws IOException {
		int maxTests = 10;
		int curTest = 0;
		bFile = openApplicationFile();
		for (int i = applicationEntries.size(); i >= 0; i--) {
			ApplicationEntry appEntry = applicationEntries.get(i);
			bOffset = appEntry.offset;
			bFile.skip(appEntry.offset);
			EpfRecord rec = nextRecord();
			Assert.assertTrue(rec.data.get(1).equals(appEntry.applicationId));
			if (curTest++ >= maxTests) {
				break;
			}
		}
		
		bFile.close();
	}
}

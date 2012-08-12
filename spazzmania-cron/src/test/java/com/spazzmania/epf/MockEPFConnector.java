package com.spazzmania.epf;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class MockEPFConnector implements EPFConnector {

	Map<String, String> urlFiles = new HashMap<String, String>();

	public MockEPFConnector() {
		urlFiles.put(EPF_BASE_URL + "/", "EPFFull_Dates.html");
		urlFiles.put(EPF_BASE_URL + "/20120801", "EPFFull_20120801_Files.html");
		urlFiles.put(EPF_BASE_URL + "/20120808", "EPFFull_20120808_Files.html");
		urlFiles.put(EPF_BASE_URL + "/20120801/incremental", "EPFIncremental_20120801_Dates.html");
		urlFiles.put(EPF_BASE_URL + "/20120808/incremental", "EPFIncremental_20120808_Dates.html");
		urlFiles.put(EPF_BASE_URL + "/20120801/incremental/20120802", "EPFIncremental_20120802_Files.html");
		urlFiles.put(EPF_BASE_URL + "/20120801/incremental/20120807", "EPFIncremental_20120807_Files.html");
	}
	
	@Override
	public void setEpfUsername(String epfUsername) {
	}

	@Override
	public void setEpfPassword(String epfPassword) {
	}

	@Override
	public void setEpfBaseUrl(String epfBaseUrl) {
	}

	@Override
	public Document getEpfPage(String epfPath) {
		InputStream htmlFile = getClass().getResourceAsStream(urlFiles.get(EPF_BASE_URL + epfPath));
		//File htmlFile = new File(urlFiles.get(EPF_BASE_URL + epfPath));
		try {
			return Jsoup.parse(htmlFile, "UTF-8", EPF_BASE_URL + epfPath);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void downloadFileFromUrl(String url, String toFilename) {
	}

}

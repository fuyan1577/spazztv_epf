package com.spazzmania.epf;

import org.jsoup.nodes.Document;

public interface EPFConnector {
	
	public static String EPF_BASE_URL = "http://feeds.itunes.apple.com/feeds/epf/v3/full";

	public void setEpfUsername(String epfUsername);
	public void setEpfPassword(String epfPassword);
	public void setEpfBaseUrl(String epfBaseUrl);
	public Document getEpfPage(String epfPath); 
	public void downloadFileFromUrl(String url, String destinationDir);

}

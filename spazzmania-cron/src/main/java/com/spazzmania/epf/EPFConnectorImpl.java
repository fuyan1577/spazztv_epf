package com.spazzmania.epf;

import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class EPFConnectorImpl implements EPFConnector {
	private String epfUsername;
	private String epfPassword;
	private boolean showProgress = false;

	public EPFConnectorImpl() {
		epfUsername = "D1g1talmunch";
		epfPassword = "ad4478914af590e2d53565c910105bf3";
	}

	public void setEpfUsername(String epfUsername) {
		this.epfUsername = epfUsername;
	}

	public void setEpfPassword(String epfPassword) {
		this.epfPassword = epfPassword;
	}

	public void setShowProgress(boolean showProgress) {
		this.showProgress = showProgress;
	}

	public Document getEpfPage(String epfPath) {
		String login = epfUsername + ":" + epfPassword;
		String base64login = new String(Base64.encodeBase64(login.getBytes()));

		Document document = null;
		try {
			document = Jsoup.connect(EPF_BASE_URL + epfPath)
					.header("Authorization", "Basic " + base64login).get();
		} catch (IOException e) {
			// Ignore and exit with a null document
		}
		return document;
	}

	public String getDownloadFilename(String urlString) {
		// Pattern pattern = Pattern.compile("([a-z]+\\d{8}\\.tbz)",
		// Pattern.MULTILINE);
		Pattern pattern = Pattern.compile(".+/([^/]+)");
		Matcher matcher = pattern.matcher(urlString);
		if (matcher.find()) {
			return matcher.group(1);
		}
		return null;
	}

	public void downloadFileFromUrl(String urlString, String destinationDir) {
		String fileName = getDownloadFilename(urlString);
		if (fileName == null) {
			throw new RuntimeException(
					"Invalid EPF URL - filename could not be determined: "
							+ urlString);
		}

		BufferedInputStream in = null;
		FileOutputStream fout = null;

		try {
			in = new BufferedInputStream(getEpfConnection(urlString)
					.getInputStream());
			fout = new FileOutputStream(destinationDir + fileName);

			System.out.println("Downloading: " + fileName);

			byte data[] = new byte[1024];
			int count;
			long totalBytes = 0;
			int progress = 0;
			while ((count = in.read(data, 0, 1024)) != -1) {
				fout.write(data, 0, count);
				totalBytes += count;
				if (showProgress) {
					if ((totalBytes / 1000000) > progress) {
						progress++;
						System.out.println(String.format("Downloaded: %d megs",
								Integer.valueOf(progress)));
					}
				}
			}
		} catch (MalformedURLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (in != null)
					in.close();
				if (fout != null)
					fout.close();
			} catch (IOException e) {
			}
		}
	}

	private HttpURLConnection getEpfConnection(String urlString) {
		String login = epfUsername + ":" + epfPassword;
		String base64login = new String(Base64.encodeBase64(login.getBytes()));

		URL url;
		HttpURLConnection connection = null;
		try {
			url = new URL(urlString);
			connection = (HttpURLConnection) url.openConnection();
			connection.addRequestProperty("Authorization", "Basic "
					+ base64login);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return connection;
	}
}

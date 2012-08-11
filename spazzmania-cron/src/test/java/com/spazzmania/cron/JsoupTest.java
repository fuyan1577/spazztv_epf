package com.spazzmania.cron;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.junit.Test;
import org.quartz.Matcher;

public class JsoupTest {

	@Test
	public void test() {
		try {
			String username = "D1g1talmunch";
			String password = "ad4478914af590e2d53565c910105bf3";
			String login = username + ":" + password;
			String base64login = new String(Base64.encodeBase64(login
					.getBytes()));

			Document document = Jsoup
					.connect("http://feeds.itunes.apple.com/feeds/epf/v3/full")
					.header("Authorization", "Basic " + base64login).get();

			Object[] hrefs = document.select("a").toArray();
			for (Object href : hrefs) {
				String h = ((Element) href).attr("href");
				if (Pattern.matches("\\d+\\/", h)) {
					System.out.println(h);
				}
			}

			document = Jsoup
					.connect("http://feeds.itunes.apple.com/feeds/epf/v3/full/20120801/incremental/")
					.header("Authorization", "Basic " + base64login).get();

			Object[] shrefs = document.select("a").toArray();
			for (Object href : shrefs) {
				String h = ((Element) href).attr("href");
				if (Pattern.matches("\\d+\\/", h)) {
					System.out.println(h);
				}
			}

			// System.out.println(document.toString());

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}

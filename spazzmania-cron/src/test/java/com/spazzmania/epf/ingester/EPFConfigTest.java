package com.spazzmania.epf.ingester;

import static org.junit.Assert.fail;

import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

public class EPFConfigTest {

	EPFConfig config;

	String epfConfigBadFields = "{" + "    \"fieldSep\": \"\u0001\", "
			+ "    \"recordSep\": \"\u0002\n\", "
			+ "    \"dbHost\": \"localhost\", " + "    \"dbName\": \"epf\", "
			+ "    \"dbUser\": \"epfimporter\", "
			+ "    \"dbPassword\": \"epf123\", "
			+ "    \"tablePrefix\": \"epf\", "
			+ "    \"allowExtensions\": false, " + "    \"blackList\": ["
			+ "        \"^\\.\"" + "    ], " + "    \"whiteList\": ["
			+ "        \".*?\"" + "    ]" + "}";

	String epfConfig1 = "{"
			+ "    \"directoryPath\": \"testdata/epf_files/\", "
			+ "    \"allowExtensions\": false, " + "    \"blackList\": ["
			+ "        \"^\\.\"" + "    ], " + "    \"whiteList\": ["
			+ "        \".*?\"" + "    ]" + "}";

	String epfConfig2 = "{" + "	\"directoryPath\" : \"testdata/epf_files/\","
			+ "    \"allowExtensions\": false, " + "    \"blackList\": ["
			+ "        \"blfile1\"," + "        \"blfile2\","
			+ "        \"blfile3\"" + "    ], " + "    \"whiteList\": ["
			+ "        \"wlfile1\"," + "        \"wlfile2\","
			+ "        \"wlfile3\"" + "    ]" + "}";

	@Test
	public void testConfig1() throws EPFImporterException {
		config = new EPFConfig();
		config.parseConfiguration(epfConfig1);
		String expectedDirectoryPath = "testdata/epf_files/";
		boolean expectedAllowExtensions = false;

		Assert.assertTrue("Invalid number of blacklist elements", config
				.getBlackList().size() == 1);
		Assert.assertTrue("Invalid number of whitelist elements", config
				.getWhiteList().size() == 1);
		Assert.assertTrue(String.format(
				"Invalid directoryPath, expected %s, actual %s",
				expectedDirectoryPath, config.getDirectoryPath()),
				expectedDirectoryPath.equals(config.getDirectoryPath()));
		Assert.assertTrue(String.format(
				"Invalid allowExtensions, expected %s, actual %s",
				Boolean.toString(expectedAllowExtensions),
				Boolean.toString(config.isAllowExtensions())),
				expectedAllowExtensions == config.isAllowExtensions());
	}

	@Test
	public void testConfig2() throws EPFImporterException {
		config = new EPFConfig();
		config.parseConfiguration(epfConfig2);
		String expectedDirectoryPath = "testdata/epf_files/";
		boolean expectedAllowExtensions = false;
		String expectedBlacklistRegex = "^blfile\\d$";
		String expectedWhitelistRegex = "^wlfile\\d$";

		Assert.assertTrue("Invalid number of blacklist elements", config
				.getBlackList().size() == 3);
		Assert.assertTrue("Invalid number of whitelist elements", config
				.getWhiteList().size() == 3);
		Assert.assertTrue(String.format(
				"Invalid directoryPath, expected %s, actual %s",
				expectedDirectoryPath, config.getDirectoryPath()),
				expectedDirectoryPath.equals(config.getDirectoryPath()));
		Assert.assertTrue(String.format(
				"Invalid allowExtensions, expected %s, actual %s",
				Boolean.toString(expectedAllowExtensions),
				Boolean.toString(config.isAllowExtensions())),
				expectedAllowExtensions == config.isAllowExtensions());

		for (String val : config.getBlackList()) {
			Assert.assertTrue(String.format(
					"Invalid blacklist element: expected %s, actual %s",
					expectedBlacklistRegex, val), val
					.matches(expectedBlacklistRegex));
		}

		for (String val : config.getWhiteList()) {
			Assert.assertTrue(String.format(
					"Invalid whitelist element: expected %s, actual %s",
					expectedWhitelistRegex, val), val
					.matches(expectedWhitelistRegex));
		}
	}

	@Test
	public void testInvalidField() {
		config = new EPFConfig();
		boolean expectedThrewError = true;
		boolean actualThrewError = false;
		try {
			config.parseConfiguration(epfConfigBadFields);
		} catch (EPFImporterException e) {
			actualThrewError = true;
		}
		Assert.assertTrue("Expected parsing to throw error",
				expectedThrewError = actualThrewError);
	}
}

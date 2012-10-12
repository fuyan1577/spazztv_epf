package com.spazzmania.epf.importer;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class EPFImporterQueueTest {

	private EPFImporterQueue importerQueue;

	private List<String> expectedImportQueue;
	private List<String> expectedFailedFiles;
	private List<String> expectedImportedFiles;

	@Before
	public void setUp() throws Exception {
		importerQueue = EPFImporterQueue.getInstance();
		expectedImportQueue = new ArrayList<String>();
		expectedFailedFiles = new ArrayList<String>();
		expectedImportedFiles = new ArrayList<String>();

		expectedImportQueue.add("success1");
		expectedImportQueue.add("success2");
		expectedImportQueue.add("success3");
		expectedImportQueue.add("failed1");
		expectedImportQueue.add("failed2");
		expectedImportQueue.add("failed3");

		expectedFailedFiles.add("failed1");
		expectedFailedFiles.add("failed2");
		expectedFailedFiles.add("failed3");

		expectedImportedFiles.add("success1");
		expectedImportedFiles.add("success2");
		expectedImportedFiles.add("success3");
	}

	@Test
	public void testAdd() {
		for (String fileName : expectedImportQueue) {
			importerQueue.add(fileName);
		}
		List<String> actualQueue = importerQueue.getImportQueue();
		Assert.assertTrue(
				String.format(
						"Wrong number of arguments in importerQueue: expected %d, actual %d",
						expectedImportQueue.size(), actualQueue.size()),
				expectedImportQueue.size() == actualQueue.size());
	}

	@Test
	public void testFailed() {
		for (String failedFile : expectedFailedFiles) {
			importerQueue.setFailed(failedFile);
		}
		List<String> actualFailedFiles = importerQueue.getFailedFiles();
		Assert.assertTrue(
				String.format(
						"Wrong number of failedFiles: expected %d, actual %d",
						expectedFailedFiles.size(), actualFailedFiles.size()),
				expectedFailedFiles.size() == actualFailedFiles.size());

		List<String> actualQueue = importerQueue.getImportQueue();
		int expectedQueueSize = expectedImportQueue.size() - expectedFailedFiles.size();
		Assert.assertTrue(
				String.format(
						"Wrong number of importerQueue files to go: expected %d, actual %d",
						expectedQueueSize, actualQueue.size()),
				expectedQueueSize == actualQueue.size());
	}

	@Test
	public void testCompleted() {
		for (String failedFile : expectedImportedFiles) {
			importerQueue.setCompleted(failedFile);
		}
		List<String> actualImportedFiles = importerQueue.getImportedFiles();
		Assert.assertTrue(
				String.format(
						"Wrong number of importedFiles: expected %d, actual %d",
						expectedImportedFiles.size(), actualImportedFiles.size()),
				expectedImportedFiles.size() == actualImportedFiles.size());
		
		List<String> actualQueue = importerQueue.getImportQueue();
		int expectedQueueSize = expectedImportQueue.size() - expectedFailedFiles.size() - expectedImportedFiles.size();
		Assert.assertTrue(
				String.format(
						"Wrong number of importerQueue files to go: expected %d, actual %d",
						expectedQueueSize, actualQueue.size()),
				expectedQueueSize == actualQueue.size());
	}

}

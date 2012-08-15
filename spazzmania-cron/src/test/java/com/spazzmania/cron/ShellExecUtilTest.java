/**
 * 
 */
package com.spazzmania.cron;

import static org.junit.Assert.fail;

import java.io.File;
import java.util.List;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

/**
 * @author billintj
 *
 */
public class ShellExecUtilTest {

	private ProcessBuilder processBuilder;
	private Logger logger;
	private String shellExecutable;
	private List<String> shellArguments;
	private File workingDirectory;
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		processBuilder = EasyMock.createMock(ProcessBuilder.class);
		
	}

	/**
	 * Test method for {@link com.spazzmania.cron.ShellExecUtil#ShellExecUtil(java.lang.ProcessBuilder, java.lang.String, java.util.List, java.io.File, org.slf4j.Logger)}.
	 */
	@Test
	public void testShellExecUtil() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for {@link com.spazzmania.cron.ShellExecUtil#start()}.
	 */
	@Test
	public void testStart() {
		fail("Not yet implemented"); // TODO
	}

}

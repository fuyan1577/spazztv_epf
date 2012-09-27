package com.spazzmania.epf.importer;

import java.util.List;

import junit.framework.Assert;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.junit.Before;
import org.junit.Test;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

public class EPFImporterTest {
	
	private EPFImporter epfImporter;
	private CmdLineParser parser;
	
	@Before
	public void setUp() throws Exception {
		epfImporter = new EPFImporter();
		parser = new CmdLineParser(epfImporter);
	}

	@Test
	public void testMainWhiteList() throws CmdLineException {
		String[] args = {"-w", "'whitelist1'","-w","'whitelist2'","--whitelist","'whitelist3'","-w","\"whitelist4\"","-b","blacklist1","--blacklist","blacklist2"};
		parser.parseArgument(args);
		
		int expectedCount = 4;
		List<String>actualWhiteList = epfImporter.getWhiteList();
		Assert.assertTrue("Invalid whiteList parsed - no values parsed",actualWhiteList != null);
		Assert.assertTrue(String.format("Invalid whitelist parsing, expecting %d whitelist items, actual %d",expectedCount,actualWhiteList.size()),actualWhiteList.size() == expectedCount);
	}

	@Test
	public void testMainBlacklist() throws CmdLineException {
		String[] args = {"-b","blacklist1","--blacklist","blacklist2"};
		parser.parseArgument(args);
		
		int expectedCount = 2;
		List<String>actualBlackList = epfImporter.getBlackList();
		Assert.assertTrue("Invalid whiteList parsed - no values parsed",actualBlackList != null);
		Assert.assertTrue(String.format("Invalid whitelist parsing, expecting %d whitelist items, actual %d",expectedCount,actualBlackList.size()),actualBlackList.size() == expectedCount);
	}
	
//	@Test
//	public void testMainBooleans() throws CmdLineException {
//		String[] args = {"-frak"};
//		parser.parseArgument(args);
//		
//		Assert.assertTrue("Invalid -f option not parsed",epfImporter.isFlat());
//		Assert.assertTrue("Invalid -r option not parsed",epfImporter.isResume());
//		Assert.assertTrue("Invalid -a option not parsed",epfImporter.isAllowExtensions());
//		Assert.assertTrue("Invalid -k option not parsed",epfImporter.isSkipKeyViolators());
//	}

	@SuppressWarnings("static-access")
	@Test
	public void testApacheParseArguments() {
		// create the command line parser
		CommandLineParser parser = new PosixParser();

		// create the Options
		Options options = new Options();
		options.addOption( "a", "all", false, "do not hide entries starting with ." );
		options.addOption( "A", "almost-all", false, "do not list implied . and .." );
		options.addOption( "b", "escape", false, "print octal escapes for nongraphic "
		                                         + "characters" );
		options.addOption( OptionBuilder.withLongOpt( "block-size" )
		                                .withDescription( "use SIZE-byte blocks" )
		                                .hasArg()
		                                .withArgName("SIZE")
		                                .create() );
		options.addOption( "B", "ignore-backups", false, "do not list implied entried "
		                                                 + "ending with ~");
		options.addOption( "c", false, "with -lt: sort by, and show, ctime (time of last " 
		                               + "modification of file status information) with "
		                               + "-l:show ctime and sort by name otherwise: sort "
		                               + "by ctime" );
		options.addOption( "C", false, "list entries by columns" );

		String[] args = new String[]{ "--block-size=10", "-aAbB" };

		try {
		    // parse the command line arguments
		    CommandLine line = parser.parse( options, args );

		    // validate that block-size has been set
		    if( line.hasOption( "block-size" ) ) {
		        // print the value of block-size
		        System.out.println( line.getOptionValue( "block-size" ) );
		        System.out.println( line.hasOption( "a" ) );
		        System.out.println( line.hasOption( "b" ) );
		        System.out.println( line.hasOption( "B" ) );
		        System.out.println( line.hasOption( "C" ) );
		    }
		}
		catch( ParseException exp ) {
		    System.out.println( "Unexpected exception:" + exp.getMessage() );
		}		
	}

}

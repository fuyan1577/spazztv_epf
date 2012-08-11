package com.spazzmania.cron;

import java.text.Format;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class ArgvPrinterJob implements SynchronizedJobClass {

	  @Argument(metaVar = "[target [target2 [target3] ...]]", usage = "options")
	  private List<String> options = new ArrayList<String>();
	 
	  @Option(name = "-h", aliases = "--help", usage = "print this message")
	  private boolean help = false;
	 
	  @Option(name = "-w", aliases = { "--wait" }, metaVar = "number", usage = "[-w | --wait] waitseconds")
	  private long waitSeconds;
	 
	  private Map<String, String> properties = new HashMap<String, String>();
	  @Option(name = "-D", metaVar = "<property>=<value>",
	          usage = "use value for given property")
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new ArgvPrinterJob().execute(args);
	}

	@Override
	public void execute(String[] args) {

	    final AntOptsArgs4J options = new AntOptsArgs4J();
	    final CmdLineParser parser = new CmdLineParser(options);
	    try {
	    	parser.parseArgument(args);
		    parser.setUsageWidth(Integer.MAX_VALUE);
	    } catch (Exception e) {
	    	throw new RuntimeException(e.getMessage());
	    }
	 
	    if (help) {
		    // print usage
		    parser.printUsage(System.err);
	    }
	    
	    if (waitSeconds > 0) {
	    	try {
	    		System.out.println(String.format("Waiting %s seconds", Long.toString(waitSeconds)));
	    		Thread.sleep(1000l * waitSeconds);
	    		System.out.println(String.format("Continuing...", Long.toString(waitSeconds)));
	    	} catch (InterruptedException e) {
	    	}
	    }
		for (String arg: args) {
			System.out.println(arg);
		}
	}
}

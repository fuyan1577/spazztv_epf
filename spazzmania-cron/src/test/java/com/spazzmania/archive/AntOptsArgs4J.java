package com.spazzmania.archive;

import static junit.framework.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
 
/**
 * import static org.junit.Assert.assertEquals;
 * Example of using Args4j for parsing
 * Ant command line options
 */
public class AntOptsArgs4J {
 
  @Option(name = "--params", metaVar = "<params>",
          usage = "parameter arguments enclosed by \"\"")
  private File params;
 
  @Option(name = "--executable", metaVar = "<executable>",
          usage = "executable script or binary")
  private File executable;
  
  @Option(name = "--directory", metaVar = "<directory>",
          usage = "base directory for execution")
  private File directory;
 
  private Map<String, String> properties = new HashMap<String, String>();
  @Option(name = "-D", metaVar = "<property>=<value>",
          usage = "job data map parameters")
  
  public void setProperty(final String property) throws CmdLineException {
    String[] arr = property.split("=");
    if(arr.length != 2) {
        throw new CmdLineException("Properties must be specified in the form:"+
                                   "<property>=<value>");
    }
    properties.put(arr[0], arr[1]);
  }
  
  public Map<String, String> getProperties() {
	  return properties;
  }
  
  public static void main(String[] args) throws CmdLineException {
    final String[] argv = { "--executable", "EPFImporter.py", "--directory","/var/www/html","--params","\"arg1 arg2 -d darg -P Parg\""};
    final AntOptsArgs4J options = new AntOptsArgs4J();
    final CmdLineParser parser = new CmdLineParser(options);
    parser.parseArgument(argv);
    
    Map<String, String> properties = options.getProperties();
    
    for (String key : properties.keySet()) {
    	System.out.println(String.format("%s = %s",key,properties.get(key)));
    }

    // print usage
    parser.setUsageWidth(Integer.MAX_VALUE);
//    parser.printUsage(System.err);
 
    // check the options have been set correctly
//    assertEquals("build.xml", options.buildFile.getName());
//    assertEquals(2, options.targets.size());
    //assertEquals(2, options.properties.size());
  }
}
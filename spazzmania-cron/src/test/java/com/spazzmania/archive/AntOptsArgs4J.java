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
 
  @Argument(metaVar = "target1 target2 [target3] ...]", usage = "targets")
  private List<String> targets = new ArrayList<String>();
 
  @Option(name = "-h", aliases = "--help", usage = "print this message")
  private boolean help = false;
 
  @Option(name = "-lib", metaVar = "<path>",
          usage = "specifies a path to search for jars and classes")
  private String lib;
 
  @Option(name = "-f", aliases = { "-file", "-buildfile" }, metaVar = "<file>",
          usage = "use given buildfile")
  private File buildFile;
 
  @Option(name = "-nice", metaVar = "number",
          usage = "A niceness value for the main thread:\n"
          + "1 (lowest) to 10 (highest); 5 is the default")
  private int nice = 5;
 
  private Map<String, String> properties = new HashMap<String, String>();
  @Option(name = "-D", metaVar = "<property>=<value>",
          usage = "use value for given property")
  
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
  
  public List<String> getTargets() {
	  return targets;
  }
  
  public static void main(String[] args) throws CmdLineException {
    final String[] argv = { "-D", "key=value", "-f", "build.xml",
                            "-D", "key2=value2", "clean", "install", "three", "four", "five" };
    final AntOptsArgs4J options = new AntOptsArgs4J();
    final CmdLineParser parser = new CmdLineParser(options);
    parser.parseArgument(argv);
    
    Map<String, String> properties = options.getProperties();
    
    for (String key : properties.keySet()) {
    	System.out.println(String.format("%s = %s",key,properties.get(key)));
    }

    // print usage
    parser.setUsageWidth(Integer.MAX_VALUE);
    parser.printUsage(System.err);
 
    // check the options have been set correctly
    assertEquals("build.xml", options.buildFile.getName());
    assertEquals(2, options.targets.size());
    assertEquals(2, options.properties.size());
  }
}
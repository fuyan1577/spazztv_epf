package com.spazzmania.archive;

import java.util.Map;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessBuilderPathTest {

	public void run() {
		String os = System.getProperty("os.name");
		String[] command;
		if (Pattern.matches("[W|w]in.+", os)) {
			command = new String[3];
			command[0] = "cmd";
			command[1] = "/C";
			command[2] = "set";
		} else {
			command = new String[2];
			command[0] = "/bin/sh";
			command[1] = "env";
		}
		ProcessBuilder builder = new ProcessBuilder(command);
		builder.redirectErrorStream(true);

		builder.environment()
				.put("Path",
						"C:\\WINDOWS\\system32;C:\\WINDOWS;C:\\WINDOWS\\System32\\Wbem;C:\\WINDOWS\\system32\\WindowsPowerShell\\v1.0;");
		for (Map.Entry<String, String> entry : builder.environment().entrySet()) {
			System.out.println(String.format("%s = %s", entry.getKey(),
					entry.getValue()));
		}
	}
	
	public static void main(String[] argv) {
		ProcessBuilderPathTest test = new ProcessBuilderPathTest();
		test.run();
	}
	
}

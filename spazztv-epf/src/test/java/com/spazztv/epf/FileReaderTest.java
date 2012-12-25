package com.spazztv.epf;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public class FileReaderTest {

	public FileReaderTest(String inputfile) throws FileNotFoundException {
		FileInputStream fstream = new FileInputStream(inputfile);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader bFile = new BufferedReader(new InputStreamReader(in));
		char[] nextChar = new char[1];
		int rows = 0;
		int cols = 0;
		try {
			// Read in the next block - read until separatorChar
			while (bFile.read(nextChar, 0, 1) > 0) {
				if (nextChar[0] == 1) {
					cols++;
				} else if (nextChar[0] == 2) {
					if (cols > 0) {
						System.out.println(String.format(
								"Row %d has %d columns", rows, cols));
						cols = 0;
					}
					rows++;
					if (rows >= 72) {
						rows = rows + 0;
					}
				}
			}
		} catch (IOException e) {
		}
	}

	public static void main(String[] args) throws FileNotFoundException {
		new FileReaderTest(args[0]);
	}
}

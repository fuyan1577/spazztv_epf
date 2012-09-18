package com.aspectj;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestTaskThreadImpl extends TestTaskThread {

	private String threadName;
	
	public TestTaskThreadImpl(String threadName) {
		this.threadName = threadName;
	}
	
	@Override
	public String getThreadName() {
		return threadName;
	}

	@Override
	public void test(int i) {
		for  (int c = 0; c < i; c++) {
			System.out.printf("TestThreadImpl %s: %d%n",threadName,c);
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				//Ignore interrupt from sleep
			}
		}
	}

}

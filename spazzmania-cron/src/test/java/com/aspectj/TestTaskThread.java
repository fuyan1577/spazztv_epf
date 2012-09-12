package com.aspectj;

import java.util.Date;
import java.util.Random;

public class TestTaskThread implements Runnable {
	private String threadName;
	
	public TestTaskThread(String threadName) {
		this.threadName = threadName;
	}
	
	public String getThreadName() {
		return threadName;
	}
	
	public void test(int i) { 
		System.out.printf("TestTaskThread %s: %d%n",threadName,i);
	}
	
	@Override
	public void run() {
		Date d = new Date();
		Random rand = new Random(d.getTime());
		for (int i = 0; i < 10; i++) {
			test(i);
			try {
//				Thread.sleep(rand.nextInt(200));
				Thread.sleep(200);
			} catch (InterruptedException e) {
				//Ignore
			}
		}
	}

}

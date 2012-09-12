package com.aspectj;

public class TestTaskLauncher {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String[] taskNames = {"A","B","C","D","E","F","G","H","I","J"};
		for (int i = 0; i < 10; i++) {
			TestTaskThread tt = new TestTaskThread(taskNames[i]);
			Thread t = new Thread(tt);
			t.start();
		}
		
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			//Ignore
		}

	}

}

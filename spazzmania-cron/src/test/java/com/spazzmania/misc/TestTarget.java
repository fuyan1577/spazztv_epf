package com.spazzmania.misc;

public class TestTarget {

    public static void main(String[] args) {
        System.out.println("----------------------->--------- Start test -----------<---------------------");
        new TestTarget().test();
        System.out.println("----------------------->--------- End test -----------<---------------------");
    }
    
    public String getId() {
    	return "DummyID1234";
    }

    public void test() {
        System.out.println("TestTarget.test()");
    }
}

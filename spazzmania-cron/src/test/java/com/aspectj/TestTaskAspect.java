package com.aspectj;


//@Aspect("perthis(adviceGetThreadName())")
public class TestTaskAspect {
	
//	@Pointcut("(call (* TestTaskThread.test(..)))")
	public void adviceGetThreadName() { }
	
//	@Before("(call (* TestTaskThread.test(..)))")
//	public void adviceGetThreadName(JoinPoint joinPoint) {
//		TestTaskThread tt = (TestTaskThread)joinPoint.getThis();
//		System.out.printf("TestTaskAspect: on %s%n", tt.getThreadName());
//	}
}
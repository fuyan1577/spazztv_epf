package com.aspectj;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;

@Aspect("perthis(adviceGetThreadName())")
public class TestTaskAspect {
	
	@Pointcut("(call (* TestTaskThread.test(..)))")
	public void adviceGetThreadName() { }
	
	@Before("(call (* TestTaskThread.test(..)))")
	public void adviceGetThreadName(JoinPoint joinPoint) {
		TestTaskThread tt = (TestTaskThread)joinPoint.getThis();
		System.out.printf("TestTaskAspect: on %s%n", tt.getThreadName());
	}
}
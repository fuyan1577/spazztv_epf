/**
 * 
 */
package com.spazztv.epf;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;

import com.spazztv.epf.dao.EPFDbWriter;

/**
 * @author tjbillingsley
 *
 */
@Aspect
public class EPFImportTaskLogger {

    @Before("call(* org.aspectprogrammer..*(..)) && this(dbWriter)")
    public void callFromFoo(JoinPoint thisJoinPoint, EPFDbWriter dbWriter) {
      System.out.println("Call from Foo: " + dbWriter + " at "
                         + thisJoinPoint);
    }
}

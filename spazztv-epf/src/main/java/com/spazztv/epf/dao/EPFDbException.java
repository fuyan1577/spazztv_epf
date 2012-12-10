/**
 * 
 */
package com.spazztv.epf.dao;

/**
 * A generic wrapper for JDBC & NoSQL DB Connection Exceptions.
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFDbException extends Exception {
	private static final long serialVersionUID = -3861587718045698586L;
	private String err;

	public EPFDbException() {
		super();
		err = "EPFDbException with data store connection";
	}

	public EPFDbException(String err) {
		super(err);
		this.err = err;
	}

	public String getError() {
		return err;
	}
}

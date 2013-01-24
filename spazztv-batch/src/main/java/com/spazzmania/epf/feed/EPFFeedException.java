package com.spazzmania.epf.feed;

/**
 * A generic wrapper for Apple Enterprise Partner Feed Exceptions
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFFeedException extends Exception {

	private String err;

	public EPFFeedException() {
		super();
		err = "EPFFeedException occurred";
	}

	public EPFFeedException(String err) {
		super(err);
		this.err = err;
	}

	public String getError() {
		return err;
	}
}

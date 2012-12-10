/**
 * 
 */
package com.spazztv.epf;

/**
 * EPF File Format exception. Thrown when the EPF Input File has an invalid
 * format.
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFFileFormatException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7058935729747920203L;
	String err;

	public EPFFileFormatException() {
		super();
		err = "EPF File Format is invalid";
	}

	public EPFFileFormatException(String err) {
		super(err);
		this.err = err;
	}

	public String getError() {
		return err;
	}
}

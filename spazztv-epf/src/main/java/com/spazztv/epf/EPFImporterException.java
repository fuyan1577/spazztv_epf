/**
 * 
 */
package com.spazztv.epf;

/**
 * Generic exception class for EPFImporter errors.
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFImporterException extends Exception {
	String err;

	public EPFImporterException() {
		super();
		err = "EPFDbWriter error writing to the data store";
	}

	public EPFImporterException(String err) {
		super(err);
		this.err = err;
	}

	public String getError() {
		return err;
	}

}

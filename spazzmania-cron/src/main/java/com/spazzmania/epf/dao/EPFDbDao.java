/**
 * 
 */
package com.spazzmania.epf.dao;

/**
 * An abstract DAO object for making calls to the database.
 * <p/>
 * This object holds the EPFDbConnector.
 * 
 * @author Thomas Billingsley
 * 
 */
public abstract class EPFDbDao {
	
	private EPFDbConnector connector;

	public EPFDbConnector getConnector() {
		return connector;
	}

	public void setConnector(EPFDbConnector connector) {
		this.connector = connector;
	}

}

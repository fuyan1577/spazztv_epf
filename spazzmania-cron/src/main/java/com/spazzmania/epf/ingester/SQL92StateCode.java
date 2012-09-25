package com.spazzmania.epf.ingester;

public class SQL92StateCode {
	
	public static String NO_DATA = "02";
	public static String CONNECTION_EXCEPTION = "08";
	public static String DYNAMIC_SQL_ERROR = "07";
	public static String FEATURE_NOT_SUPPORTED = "0A";
	public static String CARDINALITY_VIOLATION = "21";
	public static String DATA_EXCEPTION = "22";
	public static String INTEGRITY_CONSTRAINT_VIOLATION = "23";
	public static String INVALID_CURSOR_STATE = "24";
	public static String INVALID_TRANSACTION_STATE = "25";
	public static String INVALID_SQL_STATEMENT_NAME = "26";
	public static String INVALID_AUTHORIZATION_SPECIFICATION = "28";
	public static String DEPENDENT_PRIVILEGE_DESCRIPTORS_STILL_EXIST = "2B";
	public static String INVALID_CHARACTER_SET_NAME = "2C";
	public static String INVALID_TRANSACTION_TERMINATION = "2D";
	public static String INVALID_CONNECTION_NAME = "2E";
	public static String INVALID_SQL_DESCRIPTOR_NAME = "33";
	public static String INVALID_CURSOR_NAME = "34";
	public static String INVALID_CONDITION_NUMBER = "35";
	public static String INVALID_CATALOG_NAME = "3D";
	public static String AMBIGUOUS_CURSOR_NAME = "3C";
	public static String INVALID_SCHEMA_NAME = "3F";
	
	public static synchronized String getSQLStateCode(String sqlState) {
		if (sqlState == null) {
			return null;
		}
		if (sqlState.length() < 2) {
			return null;
		}
		
		return sqlState.substring(0,2);
	}

}

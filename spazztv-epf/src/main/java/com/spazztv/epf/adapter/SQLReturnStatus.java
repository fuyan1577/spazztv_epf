package com.spazztv.epf.adapter;

public class SQLReturnStatus {
	private String sqlState;
	private Integer sqlExceptionCode;
	private boolean success;
	private String description;
	
	public String getSqlStateClass() {
		if (sqlState == null) {
			return null;
		}
		if (sqlState.length() < 2) {
			return sqlState;
		}
		return sqlState.substring(0,2);
	}
	public String getSqlState() {
		return sqlState;
	}
	public void setSqlState(String sqlState) {
		this.sqlState = sqlState;
	}
	public Integer getSqlExceptionCode() {
		return sqlExceptionCode;
	}
	public void setSqlExceptionCode(Integer sqlExceptionCode) {
		this.sqlExceptionCode = sqlExceptionCode;
	}
	public boolean isSuccess() {
		return success;
	}
	public void setSuccess(boolean success) {
		this.success = success;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
}

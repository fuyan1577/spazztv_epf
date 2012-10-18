package com.spazztv.epf.adapter;

public class SQLReturnStatus {
	private String sqlStateCode;
	private Integer sqlExceptionCode;
	private boolean success;
	public String getSqlStateCode() {
		return sqlStateCode;
	}
	public void setSqlStateCode(String sqlStateCode) {
		this.sqlStateCode = sqlStateCode;
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
}

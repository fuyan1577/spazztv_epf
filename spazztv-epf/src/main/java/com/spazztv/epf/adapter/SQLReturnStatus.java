package com.spazztv.epf.adapter;

public class SQLReturnStatus {
	private String sqlStateCode;
	private Integer sqlExceptionCode;
	private boolean success;
	private String description;
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
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
}

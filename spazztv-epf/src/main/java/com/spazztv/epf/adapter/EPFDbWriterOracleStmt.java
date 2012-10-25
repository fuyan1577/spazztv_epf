package com.spazztv.epf.adapter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class EPFDbWriterOracleStmt {
	public static String DROP_TABLE_STMT = "DROP TABLE IF EXISTS %s";
	public static String CREATE_TABLE_STMT = "CREATE TABLE %s (%s)";
	public static String RENAME_TABLE_STMT = "ALTER TABLE %s RENAME TO %s";
	public static String PRIMARY_KEY_STMT = "ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s)";
	public static String INSERT_SQL_STMT = "INSERT INTO %s (%s) VALUES %s";
	public static String TABLE_EXISTS_SQL = "SHOW TABLES";
	public static String UNLOCK_TABLES = "UNLOCK TABLES";

	public static String UNION_CREATE_PT1 = "CREATE TABLE %s IGNORE SELECT * FROM %s UNION ALL ";
	public static String UNION_CREATE_PT2 = "SELECT * FROM %s WHERE 0 = (SELECT COUNT(*) FROM %s %s)";
	public static String UNION_CREATE_WHERE = "WHERE %s.export_date <= %s.export_date";
	public static String UNION_CREATE_JOIN = "AND %s.%s = %s.%s ";

	public static String MERGE_UPDATE_PT1 = "MERGE INTO %s dst USING (SELECT * FROM %s) src ON (%s)";
	public static String MERGE_UPDATE_PT2 = "WHEN MATCHED THEN UPDATE SET %s";
	public static String MERGE_UPDATE_PT3 = "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)";
	public static String MERGE_UPDATE_FIELD = "%s.%s";
	public static String MERGE_UPDATE_COND = "%s.%s = %s.%s";

	public static Map<String, String> TRANSLATION_MAP = Collections
			.unmodifiableMap(new HashMap<String, String>() {
				private static final long serialVersionUID = 1L;
				{
					put("BIGINT", "NUMBER(19, 0)");
					put("BIT", "RAW");
					put("BLOB", "BLOB");
					put("CHAR", "CHAR");
					put("DATE", "DATE");
					put("DATETIME", "DATE");
					put("DECIMAL", "FLOAT (24)");
					put("DOUBLE", "FLOAT (24)");
					put("DOUBLE PRECISION", "FLOAT (24)");
					put("ENUM", "VARCHAR2");
					put("FLOAT", "FLOAT");
					put("INT", "NUMBER(10, 0)");
					put("INTEGER", "NUMBER(10, 0)");
					put("LONGBLOB", "BLOB");
					put("LONGTEXT", "CLOB");
					put("CLOB", "CLOB");
					put("MEDIUMBLOB", "BLOB");
					put("MEDIUMINT", "NUMBER(7, 0)");
					put("MEDIUMTEXT", "CLOB");
					put("NUMERIC", "NUMBER");
					put("REAL", "FLOAT (24)");
					put("SET", "VARCHAR2");
					put("SMALLINT", "NUMBER(5, 0)");
					put("TEXT", "VARCHAR2");
					put("TIME", "DATE");
					put("TIMESTAMP", "DATE");
					put("TINYBLOB", "RAW");
					put("TINYINT", "NUMBER(3, 0)");
					put("TINYTEXT", "VARCHAR2");
					put("VARCHAR\\((\\d+)\\)", "VARCHAR2($1 CHAR)");
					put("VARCHAR", "VARCHAR2");
					put("YEAR", "NUMBER");
				}
			});

	public static List<String> UNQUOTED_TYPES = Collections
			.unmodifiableList(new ArrayList<String>() {
				private static final long serialVersionUID = 1L;
				{
					add("NUMBER.*");
				}
			});
	
	public static List<String> DATE_TYPES = Collections.unmodifiableList(new ArrayList<String>() {
		private static final long serialVersionUID = 1L;
		{
			add("DATETIME");
		}
	});

	private String translateColumnType(String columnType) {
		for (String typePat : TRANSLATION_MAP.keySet()) {
			if (columnType.matches(typePat)) {
				return columnType.replaceAll(typePat,
						TRANSLATION_MAP.get(typePat));
			}
		}
		return columnType;
	}

	public String dropTableStmt(String tableName) {
		return String.format(DROP_TABLE_STMT, tableName);
	}

	public String createTableStmt(String tableName,
			LinkedHashMap<String, String> columnsAndTypes) {
		String columnsToCreate = "";

		Iterator<Entry<String, String>> entrySet = columnsAndTypes.entrySet()
				.iterator();
		while (entrySet.hasNext()) {
			Entry<String, String> colNType = entrySet.next();
			columnsToCreate += colNType.getKey() + " "
					+ translateColumnType(colNType.getValue());

			if (entrySet.hasNext()) {
				columnsToCreate += ", ";
			}
		}

		return String.format(CREATE_TABLE_STMT, tableName, columnsToCreate);
	}

	public LinkedHashMap<String, String> setupColumnAndTypesMap(
			LinkedHashMap<String, String> columnsAndTypes,
			List<String> currentColumns) {
		// if currentColumns == null, return columnsAndTypes
		LinkedHashMap<String, String> actualColAndTypes;
		if (currentColumns == null) {
			actualColAndTypes = columnsAndTypes;
		} else {
			// Create a columns and types map for only the columns in
			// currentColumns
			actualColAndTypes = new LinkedHashMap<String, String>();
			for (String currentColumn : currentColumns) {
				if (columnsAndTypes.containsKey(currentColumn)) {
					actualColAndTypes.put(currentColumn,
							columnsAndTypes.get(currentColumn));
				}
			}
		}
		return actualColAndTypes;
	}

	public String setPrimaryKeyStmt(String tableName, List<String> keyColumns) {

		String primaryKeyColumns = "";

		Iterator<String> i = keyColumns.iterator();
		while (i.hasNext()) {
			primaryKeyColumns += "\"" + (String) i.next() + "\"";
			if (i.hasNext()) {
				primaryKeyColumns += ",";
			}
		}

		String primaryKeyName = tableName + "_pk";

		return String.format(PRIMARY_KEY_STMT, tableName, primaryKeyName,
				primaryKeyColumns);
	}

	public String insertRowStmt(String tableName,
			LinkedHashMap<String, String> columnsAndTypes,
			List<String> rowValues) {

		String insertRow = "";

		if (insertRow.length() > 0) {
			insertRow += ",";
		}
		insertRow += "(" + formatInsertRow(columnsAndTypes, rowValues) + ")";

		return String.format(INSERT_SQL_STMT, tableName,
				columnNames(columnsAndTypes), insertRow);
	}

	public String mergeTableStmt(String tableName, String incTableName,
			String unionTableName, List<String> primaryKey) {
		String mergeWhere = String.format(UNION_CREATE_WHERE, tableName,
				incTableName);

		Iterator<String> keyColumns = primaryKey.iterator();
		while (keyColumns.hasNext()) {
			String keyColumn = keyColumns.next();
			mergeWhere += " "
					+ String.format(UNION_CREATE_JOIN, tableName, keyColumn,
							incTableName, keyColumn);
		}

		String unionCreateTableSQL = String.format(UNION_CREATE_PT1,
				unionTableName, incTableName);
		unionCreateTableSQL += " "
				+ String.format(UNION_CREATE_PT2, tableName, incTableName,
						mergeWhere);

		return unionCreateTableSQL;
	}

	public String mergeUpdateTableStmt(String destTableName,
			String srcTableName, LinkedHashMap<String, String> columnsAndTypes,
			List<String> primaryKey) {
		String mergeCond = "";
		for (String column : primaryKey) {
			if (mergeCond.length() > 0) {
				mergeCond += " AND ";
			}
			mergeCond += String.format(MERGE_UPDATE_COND, "src", column, "dst",
					column);
		}
		String mergePt1 = String.format(MERGE_UPDATE_PT1, destTableName,
				srcTableName, mergeCond);

		String updateColumns = "";
		String dstColumns = "";
		String srcColumns = "";
		for (String column : columnsAndTypes.keySet()) {
			if (updateColumns.length() > 0) {
				updateColumns += ",";
				srcColumns += ",";
				dstColumns += ",";
			}
			updateColumns += String.format(MERGE_UPDATE_COND, "dst", column,
					"src", column);
			srcColumns += String.format(MERGE_UPDATE_FIELD, "src", column);
			dstColumns += String.format(MERGE_UPDATE_FIELD, "dst", column);
		}

		String mergePt2 = String.format(MERGE_UPDATE_PT2, updateColumns);
		String mergePt3 = String.format(MERGE_UPDATE_PT3, dstColumns,
				srcColumns);

		return mergePt1 + " " + mergePt2 + " " + mergePt3;
	}

	public String renameTableStmt(String srcTableName, String destTableName) {
		return String.format(RENAME_TABLE_STMT, srcTableName, destTableName);
	}

	/**
	 * Return the column names as a string for an insert statement
	 */
	private String columnNames(LinkedHashMap<String, String> columnsAndTypes) {
		StringBuilder concatList = new StringBuilder();
		for (String columnName : columnsAndTypes.keySet()) {
			if (concatList.length() > 0) {
				concatList.append(",");
			}
			concatList.append(columnName);
		}
		return concatList.toString();
	}

	private String formatInsertRow(Map<String, String> columnsAndTypes,
			List<String> rowData) {
		String row = "";
		Object[] cTypes = columnsAndTypes.values().toArray();
		for (int i = 0; i < rowData.size(); i++) {
			// Include only columns mapped in columnMap
			if (i < cTypes.length) {
				if (rowData.get(i).length() == 0) {
					row += "NULL";
				} else if (isDateType((String) cTypes[i])) {
					row += "to_date('" + rowData.get(i) + "','YYYY-MM-DD HH24:MI:SS')";
				} else if (isQuotedType((String) cTypes[i])) {
					row += "'" + rowData.get(i).replaceAll("'", "''") + "'";
				} else {
					row += rowData.get(i);
				}
				if (i + 1 < rowData.size()) {
					row += ",";
				}
			}
		}
		return row;
	}

	private boolean isQuotedType(String type) {
		for (String uqType : UNQUOTED_TYPES) {
			if (type.matches(uqType)) {
				return false;
			}
		}
		return true;
	}
	
	private boolean isDateType(String type) {
		for (String dtType : DATE_TYPES) {
			if (type.matches(dtType)) {
				return true;
			}
		}
		return false;
	}
}

package org.example.persistence;

public final class SqlIdentifier {

    private static final String IDENTIFIER_PATTERN = "^[A-Za-z0-9_]+$";

    private SqlIdentifier() {
    }

    public static String tableName(String tableName) {
        if (tableName == null || !tableName.matches(IDENTIFIER_PATTERN)) {
            throw new IllegalArgumentException("Unsafe table name: " + tableName);
        }
        return "`" + tableName + "`";
    }

    public static String columnName(String columnName) {
        if (columnName == null || !columnName.matches(IDENTIFIER_PATTERN)) {
            throw new IllegalArgumentException("Unsafe column name: " + columnName);
        }
        return "`" + columnName + "`";
    }
}

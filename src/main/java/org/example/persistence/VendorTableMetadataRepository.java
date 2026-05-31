package org.example.persistence;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.sql.DataSource;
import org.springframework.stereotype.Repository;

@Repository
public class VendorTableMetadataRepository {

    private final DataSource dataSource;

    public VendorTableMetadataRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Set<String> columns(String tableName) {
        SqlIdentifier.tableName(tableName);
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            Set<String> columns = new LinkedHashSet<>();
            try (ResultSet rs = metaData.getColumns(connection.getCatalog(), null, tableName, null)) {
                while (rs.next()) {
                    columns.add(rs.getString("COLUMN_NAME"));
                }
            }
            if (columns.isEmpty()) {
                throw new IllegalArgumentException("Configured target table does not exist: " + tableName);
            }
            return columns;
        } catch (SQLException ex) {
            throw new IllegalStateException("Unable to read metadata for table " + tableName, ex);
        }
    }

    public List<String> requiredColumns(String tableName) {
        SqlIdentifier.tableName(tableName);
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            List<String> requiredColumns = new ArrayList<>();
            try (ResultSet rs = metaData.getColumns(connection.getCatalog(), null, tableName, null)) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String nullable = rs.getString("IS_NULLABLE");
                    String autoIncrement = rs.getString("IS_AUTOINCREMENT");
                    String defaultValue = rs.getString("COLUMN_DEF");
                    if ("NO".equalsIgnoreCase(nullable)
                            && !"YES".equalsIgnoreCase(autoIncrement)
                            && defaultValue == null) {
                        requiredColumns.add(columnName);
                    }
                }
            }
            return requiredColumns;
        } catch (SQLException ex) {
            throw new IllegalStateException("Unable to read required columns for table " + tableName, ex);
        }
    }
}

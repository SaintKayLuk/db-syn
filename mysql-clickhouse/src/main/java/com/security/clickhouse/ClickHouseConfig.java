package com.security.clickhouse;

import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Saint Kay
 * @date 2022/1/18
 */
@Configuration
public class ClickHouseConfig {

    private Map<String, DbMapping> dbMappings;

    public Map<String, DbMapping> getDbMappings() {
        if (dbMappings == null) {
            dbMappings = new HashMap<>();
        }
        return dbMappings;
    }

    public void setDbMappings(Map<String, DbMapping> dbMappings) {
        this.dbMappings = dbMappings;
    }

    public class DbMapping {

        private String database;
        //database-table
        private Set<String> tables;
        //table-column
        private Map<String, Set<String>> columns;

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public Set<String> getTables() {
            if (tables == null) {
                tables = new HashSet<>();
            }
            return tables;
        }

        public void setTables(Set<String> tables) {
            this.tables = tables;
        }

        public Map<String, Set<String>> getColumns() {
            if (columns == null) {
                columns = new HashMap<>();
            }
            return columns;
        }

        public void setColumns(Map<String, Set<String>> columns) {
            this.columns = columns;
        }

    }
}

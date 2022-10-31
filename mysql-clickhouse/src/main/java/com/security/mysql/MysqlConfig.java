package com.security.mysql;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.*;

/**
 * @author Saint Kay
 * @date 2022/1/13
 */


@Configuration
@ConfigurationProperties(prefix = "binlog")
public class MysqlConfig {

    private String host;
    private int port;
    private String username;
    private String password;
    private int ServerId;
    private List<String> do_db;

    /**
     * 一个DbMapping相当于一个库
     */
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

    public int getServerId() {
        return ServerId;
    }
    public void setServerId(int serverId) {
         this.ServerId=serverId;
    }


    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public List<String> getDo_db() {
        return do_db;
    }

    public void setDo_db(List<String> do_db) {
        this.do_db = do_db;
    }

    public class DbMapping {

        private Set<String> tables;
        //table-column-true为实体字段
        private Map<String, Map<String, Boolean>> columns;
        /**
         * 表名-主键字段，只能设置一个主键
         * 多个主键存在时，取第一个
         */
        private Map<String, String> priKey;
        //table-colume-type
        private Map<String, Map<String, String>> dataType;

        public Set<String> getTables() {
            if (tables == null) {
                tables = new HashSet<>();
            }
            return tables;
        }

        public void setTables(Set<String> tables) {
            this.tables = tables;
        }

        public Map<String, Map<String, Boolean>> getColumns() {
            if (columns == null) {
                columns = new HashMap<>();
            }
            return columns;
        }

        public void setColumns(Map<String, Map<String, Boolean>> columns) {
            this.columns = columns;
        }

        public Map<String, String> getPriKey() {
            if (priKey == null) {
                priKey = new HashMap<>();
            }
            return priKey;
        }

        public void setPriKey(Map<String, String> priKey) {
            this.priKey = priKey;
        }

        public Map<String, Map<String, String>> getDataType() {
            if (dataType == null) {
                dataType = new HashMap<>();
            }
            return dataType;
        }

        public void setDataType(Map<String, Map<String, String>> dataType) {
            this.dataType = dataType;
        }
    }
}

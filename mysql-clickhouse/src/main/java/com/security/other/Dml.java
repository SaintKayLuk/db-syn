package com.security.other;

import java.util.List;
import java.util.Map;

/**
 * @author Saint Kay
 * @date 2022/2/9
 */
public class Dml {
    private String database;
    private String table;
    private String type;
    private String prikey;
    private Map<String,String> datatype;
    private List<Map<String,Object>> data;
    private String column;


    @Override
    public String toString() {
        return "Dml{" +
                "database='" + database + '\'' +
                ", table='" + table + '\'' +
                ", type='" + type + '\'' +
                ", prikey='" + prikey + '\'' +
                ", datatype=" + datatype +
                ", data=" + data +
                ", column='" + column + '\'' +
                '}';
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getPrikey() {
        return prikey;
    }

    public void setPrikey(String prikey) {
        this.prikey = prikey;
    }



    public Map<String, String> getDatatype() {
        return datatype;
    }

    public void setDatatype(Map<String, String> datatype) {
        this.datatype = datatype;
    }


    public List<Map<String, Object>> getData() {
        return data;
    }

    public void setData(List<Map<String, Object>> data) {
        this.data = data;
    }


    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }
}

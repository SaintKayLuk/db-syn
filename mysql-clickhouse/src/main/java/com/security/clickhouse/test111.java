package com.security.clickhouse;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.*;

/**
 * @author Saint Kay
 * @date 2022/1/10
 */
@Slf4j
@Component
public class test111 {

    @Autowired
    @Qualifier("clickhouseJdbcTemplate")
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private ClickHouseConfig clickHouseConfig;


    //ddl
    public void executeDdl(Map ddl) {
        String database = ddl.get("database").toString();
        String type = ddl.get("type").toString();
        String table = ddl.get("table").toString();


        if ("CREATE".equalsIgnoreCase(type)) {
            //库不存在创建库
            if (clickHouseConfig.getDbMappings().get(database) == null) {
                String sql = "CREATE DATABASE IF NOT EXISTS `" + database + "`";
                log.info("sql->" + sql);
                jdbcTemplate.execute(sql);
                clickHouseConfig.getDbMappings().put(database, clickHouseConfig.new DbMapping());
            }
            //表不存在创建表
            if (!clickHouseConfig.getDbMappings().get(database).getTables().contains(table)) {
                //todo 字段区分
                Set<String> tmpColumns = new HashSet();
                StringBuffer sql = new StringBuffer("CREATE TABLE IF NOT EXISTS `" + database + "`.`" + table + "` (");
                String id = ddl.get("prikey").toString();
                sql.append("" + id + " UInt64");
                tmpColumns.add(id);
                //todo 这里直接取名为data的字段
                ((List<Map>) ddl.get("data")).forEach(data -> {
                    Map<String, Object> map = JsonUtils.convertJsonToObject(data.get("data").toString(), Map.class);
                    if (map != null) {
                        map.forEach((k, v) -> {
                            if (tmpColumns.add(k)) {
                                sql.append("," + k + " Nullable(" + typeConvert(v) + ")");
                            }
                        });
                    }
                });
                sql.append(") ENGINE = ReplacingMergeTree() ORDER BY  (" + id + ") PRIMARY KEY (" + id + ");");
                log.info("sql->" + sql);
                jdbcTemplate.execute(sql.toString());
            }
        }

        if ("ALTER".equalsIgnoreCase(type)) {
            String sql = "alter table `" + database + "`.`" + table + "` add column " + ddl.get("column") + "";
            log.info("sql->" + sql);
            jdbcTemplate.execute(sql);
        }

        //所有ddl后直接来一遍，省力
        getColumn(database);

    }

    //dml
    public void executeDml(Map dml) {
        String database = dml.get("database").toString();
        String table = dml.get("table").toString();
        String type = dml.get("type").toString();

        StringBuffer sql = new StringBuffer();
        StringBuffer sql_1 = new StringBuffer();
        StringBuffer sql_2 = new StringBuffer();

        if (clickHouseConfig.getDbMappings().get(database) == null || !clickHouseConfig.getDbMappings().get(database).getTables().contains(table)) {
            getColumn(database);
        }
        if ("INSERT".equalsIgnoreCase(type)) {
            //insert语句时，数据库不存在创建数据库，表不存在创建表
            if ((clickHouseConfig.getDbMappings().get(database) == null || !clickHouseConfig.getDbMappings().get(database).getTables().contains(table))) {
                dml.put("type", "CREATE");
                executeDdl(dml);
            }


            List<Map<String, Object>> datas = (List) dml.get("data");
            Set<String> columns = clickHouseConfig.getDbMappings().get(database).getColumns().get(table);

            //todo 一次插入多条数据的时候，json格式的data数据会改变，导致字段增多，sql怎么去写，目前一条语句执行一次sql
            datas.forEach(data -> {
                sql.append("INSERT INTO `" + database + "`.`" + table + "` ");
                sql_1.append("(`" + dml.get("prikey") + "`");
                sql_2.append(") VALUES('" + data.get(dml.get("prikey")) + "'");
                //todo 这里直接只获取data字段数据，其他字段先不管
                if (data.get("data") != null) {
                    Map<String, Object> map = JsonUtils.convertJsonToObject(data.get("data").toString());
                    //临时set，防止json内字段和外部实体字段重复
                    Set<String> tmpColumns = new HashSet();
                    tmpColumns.add(dml.get("prikey").toString());
                    map.forEach((k, v) -> {
                        if (tmpColumns.add(k)) {
                            //字段不存在,去新增字段
                            if (!columns.contains(k)) {
                                dml.put("type", "ALTER");
                                dml.put("column", k + " Nullable(" + typeConvert(v) + ")");
                                executeDdl(dml);
                            }
                            sql_1.append(",`" + k + "`");
                            sql_2.append(",'" + v + "'");
                        }
                    });
                }
                sql.append(sql_1).append(sql_2).append(");");
                log.info("sql->" + sql);
                jdbcTemplate.execute(sql.toString());
            });
        }

        if ("UPDATE".equalsIgnoreCase(type)) {
            //update操作，库或者表都不存在，忽略
            if ((clickHouseConfig.getDbMappings().get(database) == null || !clickHouseConfig.getDbMappings().get(database).getTables().contains(table))) {
                return;
            }

            List<Map<String, Object>> datas = (List) dml.get("data");
            Set<String> columns = clickHouseConfig.getDbMappings().get(database).getColumns().get(table);
            datas.forEach(data -> {
                sql.append("ALTER TABLE `" + database + "`.`" + table + "` UPDATE ");
                if (data.get("data") == null) {
                    return;
                }
                //先判断有无新字段，新字段去添加字段
                Map<String, Object> map = JsonUtils.convertJsonToObject(data.get("data").toString());
                map.forEach((k, v) -> {
                    //字段不存在,去新增字段
                    if (!columns.contains(k)) {
                        dml.put("type", "ALTER");
                        dml.put("column", k + " Nullable(" + typeConvert(v) + ")");
                        executeDdl(dml);
                    }
                });
                //临时set，防止json内字段和外部实体字段重复
                Set<String> tmpColumns = new HashSet();
                tmpColumns.add(dml.get("prikey").toString());
                map.forEach((k, v) -> {
                    if (tmpColumns.add(k)) {
                        sql.append("`" + k + "`='" + v + "',");
                    }
                });


                sql.delete(sql.length() - 1, sql.length());
                sql.append(" WHERE `" + dml.get("prikey") + "`=" + data.get(dml.get("prikey")) + "");
                log.info("sql->" + sql);
                jdbcTemplate.execute(sql.toString());
            });


        }
    }


    //java类型转换为clickhouse里面的类型
    private List<String> typeConvert(Object o) {
        List<String> list=new ArrayList<>();
        if (o instanceof Long || o instanceof Integer) {
            list.add("UInt64");
        }
        if (o instanceof BigDecimal || o instanceof Double) {
            list.add("Decimal64(2)");
        }
        //二级json对象
        if(o instanceof  Map){
            return null;
        }
        return list;
    }

    //获取数据库表，字段信息
    private void getColumn(String schema) {
        Map<String, ClickHouseConfig.DbMapping> dbMappings = clickHouseConfig.getDbMappings();
        String sql = "SELECT `table`,`name` FROM system.columns WHERE `database` = '" + schema + "'";
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        if (list.size() > 0) {
            ClickHouseConfig.DbMapping dbMapping = clickHouseConfig.new DbMapping();
            dbMapping.setDatabase(schema);
            clickHouseConfig.getDbMappings().put(schema, dbMapping);
            for (Map map : list) {
                String table = map.get("table").toString();
                String column = map.get("name").toString();
                Set<String> tables = dbMapping.getTables();
                tables.add(table);
                if (dbMapping.getColumns().get(table) == null) {
                    dbMapping.getColumns().put(table, new HashSet<>());
                }
                dbMapping.getColumns().get(table).add(column);
            }
        }

    }

}

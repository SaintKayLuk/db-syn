package com.security.clickhouse;

import com.security.other.Dml;
import com.security.other.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.*;

/**
 * @author Saint Kay
 * @date 2022/1/10
 */
@Slf4j
@Component
public class ClickhouseService {


    @Resource(name = "clickhouseJdbcTemplate")
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private ClickHouseConfig clickHouseConfig;


    //ddl
    public void executeDdl(Dml ddl) {
        String database = ddl.getDatabase();
        String type = ddl.getType();
        String table = ddl.getTable();


        if ("CREATE".equalsIgnoreCase(type)) {
            //库不存在创建库
            if (clickHouseConfig.getDbMappings().get(database) == null) {
                String sql = "CREATE DATABASE IF NOT EXISTS `" + database + "`";
                log.info("sql->" + sql);
                jdbcTemplate.execute(sql);
                clickHouseConfig.getDbMappings().put(database, clickHouseConfig.new DbMapping());
            }
            //表不存在创建表,创建表只创建主键字段
            if (!clickHouseConfig.getDbMappings().get(database).getTables().contains(table)) {
                //StringBuffer sql = new StringBuffer("CREATE TABLE IF NOT EXISTS `" + database + "`.`" + table + "` (");
                if (ddl.getData().size() > 0) {
                    Map<String, Object> map = ddl.getData().get(0);
                    String id = ddl.getPrikey();
                    String sql = "CREATE TABLE IF NOT EXISTS `" + database + "`.`" + table + "` (" + id + " " + typeConvert(map.get(ddl.getPrikey())) + ") ENGINE = ReplacingMergeTree() ORDER BY (" + id + ") PRIMARY KEY (" + id + ");";
                    log.info("sql->" + sql);
                    jdbcTemplate.execute(sql);
                }


            }
        }

        if ("ALTER".equalsIgnoreCase(type)) {
            String sql = "alter table `" + database + "`.`" + table + "` add column " + ddl.getColumn() + "";
            log.info("sql->" + sql);
            jdbcTemplate.execute(sql);
        }

        //所有ddl后直接来一遍，省力
        getColumn(database);

    }

    //dml
    public void executeDml(Dml dml) {
        String database = dml.getDatabase();
        String type = dml.getType();
        String table = dml.getTable();

        if (clickHouseConfig.getDbMappings().get(database) == null || !clickHouseConfig.getDbMappings().get(database).getTables().contains(table)) {
            getColumn(database);
        }

        if ("INSERT".equalsIgnoreCase(type)) {
            //insert语句时，数据库不存在创建数据库，表不存在创建表
            if ((clickHouseConfig.getDbMappings().get(database) == null || !clickHouseConfig.getDbMappings().get(database).getTables().contains(table))) {
                dml.setType("CREATE");
                executeDdl(dml);
            }


            List<Map<String, Object>> datas = dml.getData();
            //todo 一次插入多条数据的时候，json格式的data数据会改变，导致字段增多，sql怎么去写，目前一条语句执行一次sql
            datas.forEach(data -> {
                if (data == null) {
                    return;
                }
                //先判断有无新字段，新字段去添加字段
                judgeField(dml, data);
                dmlInsert(dml, data);
            });
        }

        if ("UPDATE".equalsIgnoreCase(type)) {
            //update操作，库或者表不存在，忽略
            if ((clickHouseConfig.getDbMappings().get(database) == null || !clickHouseConfig.getDbMappings().get(database).getTables().contains(table))) {
                return;
            }

            List<Map<String, Object>> datas = dml.getData();
            datas.forEach(data -> {
                if (data == null) {
                    return;
                }
                judgeField(dml, (Map) data.get("after"));
                dmlUpdate(dml, data);
            });
        }

        if ("DELETE".equalsIgnoreCase(type)) {
            //delete操作，库或者表不存在，忽略
            if ((clickHouseConfig.getDbMappings().get(database) == null || !clickHouseConfig.getDbMappings().get(database).getTables().contains(table))) {
                return;
            }
            List<Map<String, Object>> datas = dml.getData();
            datas.forEach(data -> {
                dmlDelete(dml, data);
            });
        }


    }

    private void dmlInsert(Dml dml, Map<String, Object> data) {
        String database = dml.getDatabase();
        String table = dml.getTable();

        StringBuffer sql = new StringBuffer();
        StringBuffer sql_1 = new StringBuffer();
        StringBuffer sql_2 = new StringBuffer();
        sql.append("INSERT INTO `" + database + "`.`" + table + "` ");
        sql_1.append("(`" + dml.getPrikey() + "`");
        sql_2.append(") VALUES('" + data.get(dml.getPrikey()) + "'");


        //临时set，防止json内字段和外部实体字段重复
        Set<String> tmpColumns = new HashSet();
        tmpColumns.add(dml.getPrikey());
        tmpColumns.add("data");

        //拼接普通字段
        data.forEach((k, v) -> {
            if (tmpColumns.add(k)) {
                sql_1.append(",`" + k + "`");
                if ("String".equalsIgnoreCase(typeConvert(v))) {
                    sql_2.append(",'" + v + "'");
                } else {
                    sql_2.append("," + v + "");
                }
            }
        });

        //拼接data字段
        if (data.get("data") != null) {
            Map<String, Object> map = JsonUtils.convertJsonToObject(data.get("data").toString());
            map.forEach((k, v) -> {
                if (v instanceof Map) {
                    Map m = (LinkedHashMap) v;
                    m.forEach((kk, vv) -> {
                        if (tmpColumns.add(k + "_" + kk)) {
                            sql_1.append(",`" + k + "_" + kk + "`");
                            if ("String".equalsIgnoreCase(typeConvert(vv))) {
                                sql_2.append(",'" + vv + "'");
                            } else {
                                sql_2.append("," + vv + "");
                            }
                        }
                    });
                } else {
                    if (tmpColumns.add(k)) {
                        sql_1.append(",`" + k + "`");
                        if ("String".equalsIgnoreCase(typeConvert(v))) {
                            sql_2.append(",'" + v + "'");
                        } else {
                            sql_2.append("," + v + "");
                        }
                    }
                }
            });
        }


        sql.append(sql_1).append(sql_2).append(");");
        log.info("sql->" + sql);
        jdbcTemplate.execute(sql.toString());
    }

    private void dmlUpdate(Dml dml, Map<String, Object> data) {
        Map<String, Object> beforeData = (Map<String, Object>) data.get("before");
        Map<String, Object> afterData = (Map<String, Object>) data.get("after");


        //  if (beforeData.get(dml.getPrikey()) != afterData.get(dml.getPrikey())) {
        dmlDelete(dml, beforeData);
        dmlInsert(dml, afterData);
//        } else {
//            StringBuffer sql = new StringBuffer();
//            sql.append("ALTER TABLE `" + dml.getDatabase() + "`.`" + dml.getTable() + "` UPDATE ");
//            //临时set，防止json内字段和外部实体字段重复
//            Set<String> tmpColumns = new HashSet();
//            tmpColumns.add(dml.getPrikey());
//            tmpColumns.add("data");
//
//
//            //拼接普通字段
//            afterData.forEach((k, v) -> {
//                if (tmpColumns.add(k)) {
//                    if ("String".equalsIgnoreCase(typeConvert(v))) {
//                        sql.append("`" + k + "`='" + v + "',");
//                    } else {
//                        sql.append("`" + k + "`=" + v + ",");
//                    }
//                }
//            });
//
//            if (afterData.get("data") != null) {
//                Map<String, Object> map = JsonUtils.convertJsonToObject(afterData.get("data").toString());
//                map.forEach((k, v) -> {
//                    if (v instanceof Map) {
//                        Map m = (LinkedHashMap) v;
//                        m.forEach((kk, vv) -> {
//                            if (tmpColumns.add(k + "_" + kk)) {
//                                if ("String".equalsIgnoreCase(typeConvert(vv))) {
//                                    sql.append("`" + k + "_" + kk + "`='" + vv + "',");
//                                } else {
//                                    sql.append("`" + k + "_" + kk + "`=" + vv + ",");
//                                }
//
//                            }
//                        });
//                    } else {
//                        if (tmpColumns.add(k)) {
//                            if ("String".equalsIgnoreCase(typeConvert(v))) {
//                                sql.append("`" + k + "`='" + v + "',");
//                            } else {
//                                sql.append("`" + k + "`=" + v + ",");
//                            }
//                        }
//                    }
//                });
//            }
//
//            sql.delete(sql.length() - 1, sql.length());
//            sql.append(" WHERE `" + dml.getPrikey() + "` = '" + beforeData.get(dml.getPrikey()) + "'");
//            log.info("sql->" + sql);
//            jdbcTemplate.execute(sql.toString());
//        }


    }

    private void dmlDelete(Dml dml, Map<String, Object> data) {
        String sql = "ALTER TABLE `" + dml.getDatabase() + "`.`" + dml.getTable() + "` DELETE WHERE " + dml.getPrikey() + " = '" + data.get(dml.getPrikey()) + "'";
        log.info("sql->" + sql);
        jdbcTemplate.execute(sql);
    }

    //判断有无新增字段，有新字段则添加
    private void judgeField(Dml dml, Map<String, Object> data) {

        //List<Map<String, Object>> datas = dml.getData();
        //datas.forEach(data1 -> {
        data.forEach((dataK, dataV) -> {
            Set<String> columns = clickHouseConfig.getDbMappings().get(dml.getDatabase()).getColumns().get(dml.getTable());
            //todo 只转换data字段的json，其他字段为json也当String处理
            if (dataK.equalsIgnoreCase("data") && dataV != null) {
                Map<String, Object> map = JsonUtils.convertJsonToObject(dataV.toString());
                map.forEach((k, v) -> {
                    if (v instanceof Map) {
                        //二级json对象
                        Map m = (LinkedHashMap) v;
                        m.forEach((kk, vv) -> {
                            //字段不存在,去新增字段
                            if (!columns.contains(k + "_" + kk)) {
                                dml.setType("ALTER");
                                dml.setColumn("`" + k + "_" + kk + "` Nullable(" + typeConvert(vv) + ")");
                                executeDdl(dml);
                            }
                        });
                    } else {
                        if (!columns.contains(k)) {
                            dml.setType("ALTER");
                            dml.setColumn("`" + k + "` Nullable(" + typeConvert(v) + ")");
                            executeDdl(dml);
                        }
                    }
                });
            } else {
                //普通字段
                if (!columns.contains(dataK)) {
                    dml.setType("ALTER");
                    dml.setColumn("`" + dataK + "` Nullable(" + typeConvert(dataV) + ")");
                    executeDdl(dml);
                }
            }

        });
        //});

    }

    //java类型转换为clickhouse里面的类型
    private String typeConvert(Object o) {
        if (o instanceof Long || o instanceof Integer) {
            return "UInt64";
        }
        if (o instanceof BigDecimal || o instanceof Double) {
            return "Decimal64(2)";
        }
        return "String";
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

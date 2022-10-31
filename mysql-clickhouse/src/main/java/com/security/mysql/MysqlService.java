package com.security.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary;
import com.security.clickhouse.ClickhouseService;
import com.security.other.Dml;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.system.ApplicationHome;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.*;
import java.util.*;

/**
 * @author Saint Kay
 * @date 2022/1/12
 */
@Slf4j
@Component
public class MysqlService {

    //项目路径
    final String path = new ApplicationHome(this.getClass()).getSource().getParentFile().toString();

    String filename;
    Map<String, String> tempMap = new HashMap<>();

    @Autowired
    private MysqlConfig mysqlConfig;
    @Resource(name = "mysqlJdbcTemplate")
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private ClickhouseService ClickhouseService;

    @PostConstruct
    public void test() throws IOException {
        getColumn();

        //mysql连接参数
        BinaryLogClient client = new BinaryLogClient(mysqlConfig.getHost(), mysqlConfig.getPort(), mysqlConfig.getUsername(), mysqlConfig.getPassword());
        //binlog参数
        client.setServerId(mysqlConfig.getServerId());
        //用一个文件记录当前日志文件和偏移量
        File filename_position = new File(this.path + "/filename_position");

        if (filename_position.exists() && filename_position.isFile()) {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filename_position), "UTF-8"));
            client.setBinlogFilename(br.readLine());
            client.setBinlogPosition(Integer.valueOf(br.readLine()));
        } else {
            filename_position.createNewFile();
        }


        //todo 多条相同的update或者insert语句合并，合并到一个map
        client.registerEventListener(new BinaryLogClient.EventListener() {
            @Override
            public void onEvent(Event event) {

                //System.out.println(event.toString());

                if ("ROTATE".equalsIgnoreCase(event.getHeader().getEventType().toString())) {
                    //记录新日志文件和偏移量
                    RotateEventData rotateEventData = (RotateEventData) event.getData();
                    filename = rotateEventData.getBinlogFilename();
                    try {
                        FileWriter fw = new FileWriter(MysqlService.this.path + "/filename_position", false);
                        fw.write(rotateEventData.getBinlogFilename() + "\r\n" + rotateEventData.getBinlogPosition());
                        fw.close();
                    } catch (IOException e) {
                    }

                }
                if ("TABLE_MAP".equalsIgnoreCase(event.getHeader().getEventType().toString())) {
                    TableMapEventData tableMap = (TableMapEventData) event.getData();
                    tempMap.put("database", tableMap.getDatabase());
                    tempMap.put("table", tableMap.getTable());
                }
                //dml
                if (EventType.isRowMutation(event.getHeader().getEventType())) {
                    //System.err.println(event.toString());
                    Dml dml = executeDml(event);
                    if (dml != null) {
                        //new Thread(() -> ClickhouseService.executeDml(dml)).start();
                        ClickhouseService.executeDml(dml);
                    }
                    //记录偏移量
                    EventHeaderV4 header = (EventHeaderV4) event.getHeader();
                    try {
                        FileWriter fw = new FileWriter(MysqlService.this.path + "/filename_position", false);
                        fw.write(filename + "\r\n" + header.getNextPosition());
                        fw.close();
                    } catch (IOException e) {
                    }

                }
                //ddl
                //todo 没有好的判断是ddl操作的方式
                if (event.getHeader().getEventType() == EventType.QUERY) {
                    QueryEventData queryEventData = (QueryEventData) event.getData();
                    if (!"BEGIN".equalsIgnoreCase(queryEventData.getSql())) {
                        executeDdl(event);
                    }
                }


            }
        });
        client.connect();
    }


    private Dml executeDml(Event event) {
        //没有此数据库，即此数据库不需要同步
        if (mysqlConfig.getDbMappings().get(tempMap.get("database")) == null) {
            return null;
        }

        Dml dml = new Dml();
        dml.setDatabase(tempMap.get("database").toString());
        dml.setTable(tempMap.get("table"));
        dml.setPrikey(mysqlConfig.getDbMappings().get(dml.getDatabase()).getPriKey().get(dml.getTable()));

        Map<String, Boolean> columnsMap = mysqlConfig.getDbMappings().get(dml.getDatabase()).getColumns().get(dml.getTable());
        Map<String, String> dataTypeMap = mysqlConfig.getDbMappings().get(dml.getDatabase()).getDataType().get(dml.getTable());
        List<Map<String, Object>> datas = new ArrayList<>();
        HashMap<String, String> types = new HashMap<>();
        //insert
        if (EventType.isWrite(event.getHeader().getEventType())) {
            dml.setType("INSERT");
            WriteRowsEventData writeRowsEventData = (WriteRowsEventData) event.getData();
            for (Serializable[] row : writeRowsEventData.getRows()) {
                List dataList = new ArrayList<>(Arrays.asList(row));
                HashMap<String, Object> dataMap = new HashMap<>();
                int i = 0;
                for (Map.Entry<String, Boolean> entry : columnsMap.entrySet()) {
                    //添加非虚拟字段
                    if (entry.getValue()) {
                        //json字段
                        if ("json".equalsIgnoreCase(dataTypeMap.get(entry.getKey()))) {
                            byte[] b = (byte[]) dataList.get(i);
                            try {
                                dataMap.put(entry.getKey(), b != null && b.length > 0 ? JsonBinary.parseAsString(b) : null);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        } else {
                            //普通字段
                            dataMap.put(entry.getKey(), dataList.get(i) == null ? null : dataList.get(i).toString());
                        }
                        //字段类型
                        types.put(entry.getKey(), dataTypeMap.get(entry.getKey()));
                    }
                    i++;
                }
                datas.add(dataMap);
            }
        }
        //update
        if (EventType.isUpdate(event.getHeader().getEventType())) {
            dml.setType("UPDATE");
            UpdateRowsEventData updateRowsEventData = (UpdateRowsEventData) event.getData();
            for (Map.Entry<Serializable[], Serializable[]> row : updateRowsEventData.getRows()) {
                Serializable[] key = row.getKey();
                Serializable[] value = row.getValue();

                List beforeList = new ArrayList<>(Arrays.asList(key));
                List afterList = new ArrayList<>(Arrays.asList(value));

                HashMap<String, Object> beforeMap = new HashMap<>();
                HashMap<String, Object> afterMap = new HashMap<>();

                int i = 0;
                for (Map.Entry<String, Boolean> entry : columnsMap.entrySet()) {
                    //旧数据记录主键
                    if (dml.getPrikey().equalsIgnoreCase(entry.getKey())) {
                        beforeMap.put(entry.getKey(), beforeList.get(i).toString());
                    }
                    //添加非虚拟字段
                    if (entry.getValue()) {
                        //json字段
                        if ("json".equalsIgnoreCase(dataTypeMap.get(entry.getKey()))) {
                            byte[] b = (byte[]) afterList.get(i);
                            try {
                                afterMap.put(entry.getKey(), b.length > 0 ? JsonBinary.parseAsString(b) : null);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        } else {
                            //普通字段
                            afterMap.put(entry.getKey(), afterList.get(i) == null ? null : afterList.get(i).toString());
                        }
                        //字段类型
                        types.put(entry.getKey(), dataTypeMap.get(entry.getKey()));
                    }
                    i++;
                }
                datas.add(new HashMap<String, Object>() {{
                    put("before", beforeMap);
                    put("after", afterMap);
                }});

            }
        }
        //delete
        if (EventType.isDelete(event.getHeader().getEventType())) {
            dml.setType("DELETE");
            DeleteRowsEventData deleteRowsEventData = (DeleteRowsEventData) event.getData();
            for (Serializable[] row : deleteRowsEventData.getRows()) {
                List dataList = new ArrayList<>(Arrays.asList(row));
                HashMap<String, Object> dataMap = new HashMap<>();
                int i = 0;
                for (Map.Entry<String, Boolean> entry : columnsMap.entrySet()) {
                    if (entry.getValue()) {
                        //delete操作只记录id
                        if (dml.getPrikey().equalsIgnoreCase(entry.getKey())) {
                            dataMap.put(entry.getKey(), dataList.get(i).toString());
                        }
                    }
                    i++;
                }
                datas.add(dataMap);
            }
        }
        dml.setData(datas);
        dml.setDatatype(types);
        log.info("dml-->" + dml.toString());
        return dml;
    }


    private void executeDdl(Event event) {
        QueryEventData queryEventData = (QueryEventData) event.getData();
        //没有此数据库，即此数据库不需要同步
        if (mysqlConfig.getDbMappings().get(queryEventData.getDatabase()) == null) {
            return;
        }

        //todo 不管什么ddl，先重新获取一遍字段，更新mysqlConfig，省力
        getColumn();
//        Map<String, Object> map = new HashMap<>();
//        map.put("database", queryEventData.getDatabase());
//        map.put("sql", queryEventData.getSql());

        //System.out.println(map.toString());

    }

    /**
     * 获取mysql字段
     */
    private void getColumn() {
        for (String schema : mysqlConfig.getDo_db()) {
            String sql = "SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,COLUMN_KEY,GENERATION_EXPRESSION,DATA_TYPE FROM information_schema.`COLUMNS` WHERE TABLE_SCHEMA = '" + schema + "'";
            List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
            MysqlConfig.DbMapping dbMapping = mysqlConfig.new DbMapping();
            mysqlConfig.getDbMappings().put(schema, dbMapping);
            //遍历sql结果，添加每个字段
            for (Map map : list) {
                String table = map.get("TABLE_NAME").toString();
                String column = map.get("COLUMN_NAME").toString();
                String key = map.get("COLUMN_KEY").toString();
                String virtual = map.get("GENERATION_EXPRESSION").toString();
                String dataType = map.get("DATA_TYPE").toString();
                mysqlConfig.getDbMappings().forEach((database, db) -> {
                    Set<String> tables = db.getTables();
                    if (!tables.contains(table)) {
                        tables.add(table);
                    }
                    if (db.getColumns().get(table) == null) {
                        db.getColumns().put(table, new LinkedHashMap<>());
                    }
                    db.getColumns().get(table).put(column, "".equalsIgnoreCase(virtual) ? true : false);

                    if (db.getDataType().get(table) == null) {
                        db.getDataType().put(table, new HashMap<>());
                    }
                    db.getDataType().get(table).put(column, dataType);

                    //todo 多个主键怎么取，目前取第一个
                    if ("PRI".equalsIgnoreCase(key) && db.getPriKey().get(table) == null) {
                        db.getPriKey().put(table, column);
                    }


                });
            }
        }

    }

}

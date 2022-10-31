package com.byd;

import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class JDBCApp {
    public static void main(String[] args) throws Exception {
        try {
            runAPP();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void runAPP() throws Exception {
        // flink tenv
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.executeSql("CREATE TABLE t1(" +
                "  id INT PRIMARY KEY NOT ENFORCED," +
                "  name VARCHAR(10)," +
                "  age INT," +
                "  `partition` VARCHAR(20)" +
                ")" +
                "PARTITIONED BY (`partition`)" +
                "WITH (" +
                "  'connector'='hudi'," +
                "  'path'='file:///Users/looper/Desktop/hudi/t1'," +
                "  'table.type'='MERGE_ON_READ' " +
                ")");
        Table t1 = tenv.from("t1");
        t1.execute().print();
    }
}

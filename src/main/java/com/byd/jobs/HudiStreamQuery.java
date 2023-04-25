package com.byd.jobs;

import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;

public class HudiStreamQuery {
    public static void main(String[] args) {
        try {
            runAPP();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void runAPP() {
        // flink tenv
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tenv = TableEnvironment.create(settings);
        tenv.executeSql("CREATE TABLE t1(" +
                "  id BIGINT PRIMARY KEY NOT ENFORCED," +
                "  name STRING," +
                "  age INT," +
                "  birthday DATE," +
                "  ts TIMESTAMP(3)," +
                "  money DECIMAL(16,2)," +
                "  `partition` STRING" +
                ")" +
                "PARTITIONED BY (`partition`)" +
                "WITH (" +
                "  'connector'='hudi'," +
                "  'path'='file:///d://hudi/t1'," +
                "  'table.type'='MERGE_ON_READ'," +
                "'read.streaming.enabled'='true'," +
                "'read.start-commit'='20221031095157'," +
                "'read.streaming.check-interval'='5'" +
                ")");
        Table t1 = tenv.from("t1");
        tenv.executeSql("desc t1");
    }
}

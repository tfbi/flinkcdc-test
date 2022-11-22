package com.byd;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

public class Mysql2Hudi {
    public static void main(String[] args) {

        DataType dataType = DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).bridgedTo(TimestampData.class);
        System.out.println(dataType);
    }
}

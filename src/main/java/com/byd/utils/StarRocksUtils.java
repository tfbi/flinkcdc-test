package com.byd.utils;

import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.hudi.util.HoodiePipeline;

import java.util.HashMap;
import java.util.Map;

public class StarRocksUtils {
    public static SinkFunction<String> createStarRocksSinkFunction(String dbName, String tableName, Map<String, String> properties) {
        StarRocksSinkOptions.Builder builder = StarRocksSinkOptions.builder();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            builder.withProperty(entry.getKey(), entry.getValue());
        }
        builder.withProperty("table-name", tableName);
        builder.withProperty("database-name", dbName);
        return StarRocksSink.sink(builder.build());
    }

    public static Map<String, SinkFunction<String>> createStarRocksSinkMap(
            String sourceDb,
            String starRockDb,
            Map<String, String> tableMap,
            Map<String, String> properties) {
        Map<String, SinkFunction<String>> starRocksSinkMap = new HashMap<>();

        for (Map.Entry<String, String> entry : tableMap.entrySet()) {
            String sourceTbName = entry.getKey();
            String starRocksTbName = entry.getValue();
            SinkFunction<String> sink = createStarRocksSinkFunction(starRockDb, starRocksTbName, properties);
            starRocksSinkMap.put(sourceDb + "." + sourceTbName, sink);
        }
        return starRocksSinkMap;
    }
}

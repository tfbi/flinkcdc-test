package com.byd.jobs;

import com.byd.schema.DefaultSchemaConverter;
import com.byd.schema.RecordInfo;
import com.byd.schema.RecordInfoDebeziumDeserializationSchema;
import com.byd.utils.CheckpointUtils;
import com.byd.utils.StarRocksUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Cdc2StartRocks {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        CheckpointUtils.setCheckpoint(env, "file:///d:/ck");
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(5), CheckpointingMode.EXACTLY_ONCE);
        DataStreamSource<RecordInfo> source = env.fromSource(createSource(), WatermarkStrategy.noWatermarks(), "mysql");

        SingleOutputStreamOperator<String> stream = source.map(new MapFunction<RecordInfo, String>() {
            @Override
            public String map(RecordInfo value) throws Exception {
                return value.getRow().toJsonWithKind();
            }
        });
        stream.printToErr(">>>");

        Map<String, String> properties = new HashMap<>();
        properties.put("jdbc-url", "jdbc:mysql://10.17.16.77:9030/db_test?useUnicode=true&characterEncoding=UTF-8");
        properties.put("load-url", "10.17.16.77:8030");
        properties.put("username", "bigdata");
        properties.put("password", "123456");
        properties.put("sink.properties.format", "json");
        properties.put("sink.properties.strip_outer_array", "true");
        properties.put("sink.parallelism", "1");

        SinkFunction<String> sink = StarRocksUtils.createStarRocksSinkFunction("db_test","sys_user",properties  );

        stream.addSink(sink);
        env.execute();
    }

    public static MySqlSource<RecordInfo> createSource() {
        MySqlSourceBuilder<RecordInfo> builder = MySqlSource.builder();
        Properties prop = new Properties();
        prop.setProperty("decimal.handling.mode", "string");
        builder.hostname("10.17.7.197")
                .port(3306)
                .scanNewlyAddedTableEnabled(true)
                .databaseList("test")
                .username("canal")
                .password("Bigdata@123")
                .tableList("test.stu")
                .startupOptions(StartupOptions.initial())
                .includeSchemaChanges(true)
                .debeziumProperties(prop)
                .deserializer(new RecordInfoDebeziumDeserializationSchema(new DefaultSchemaConverter(true)));
        return builder.build();
    }
}

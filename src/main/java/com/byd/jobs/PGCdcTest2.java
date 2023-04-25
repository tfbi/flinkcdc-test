package com.byd.jobs;

import com.byd.schema.DefaultSchemaConverter;
import com.byd.schema.RecordInfo;
import com.byd.schema.RecordInfoDebeziumDeserializationSchema;
import com.byd.utils.CheckpointUtils;
import com.byd.utils.StarRocksUtils;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
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

public class PGCdcTest2 {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
//        config.setString("execution.savepoint.path", "file:///d:/ck/ea67cd560d5505be013e74857410a3cc/chk-2");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        CheckpointUtils.setCheckpoint(env, "file:///d:/ck");
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(10), CheckpointingMode.EXACTLY_ONCE);
        Properties prop = new Properties();
        prop.setProperty("plugin.name", "pgoutput");
        prop.setProperty("slot.name", "flink_pg_test_1");
        PostgresSourceBuilder<RecordInfo> build = new PostgresSourceBuilder<RecordInfo>();
        PostgresSourceBuilder.PostgresIncrementalSource<RecordInfo> source = build
                .hostname("10.43.89.8")
                .port(5432)
                .database("postgres")
                .schemaList("public")
                .tableList("public.biz_lead")
                .username("postgres")
                .password("123")
                .chunkKeyColumn("id")
                .debeziumProperties(prop)
                .deserializer(new RecordInfoDebeziumDeserializationSchema(new DefaultSchemaConverter(true))) // converts SourceRecord to JSON String
                .build();
//        PostgreSQLSource.Builder<RecordInfo> builder = PostgreSQLSource.builder();
//        SourceFunction<RecordInfo> sourceFunction = builder
//                .hostname("10.17.11.77")
//                .port(5432)
//                .database("online-disk-master") // monitor postgres database
//                .schemaList("odm_user")  // monitor inventory schema
//                .tableList("odm_user.file[\\d+]") // monitor products table
//                .username("ods_user")
//                .password("123#byd")
//                .debeziumProperties(prop)
//                .deserializer(new RecordInfoDebeziumDeserializationSchema(new DefaultSchemaConverter(true))) // converts SourceRecord to JSON String
//                .build();

        DataStreamSource<RecordInfo> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "pg");
        SingleOutputStreamOperator<String> stream = sourceStream.map(new MapFunction<RecordInfo, String>() {
            @Override
            public String map(RecordInfo recordInfo) throws Exception {
                return recordInfo.getRow().toJsonWithKind();
            }
        });
        stream.printToErr(">>>");
//        Map<String, String> properties = new HashMap<>();
//        properties.put("jdbc-url", "jdbc:mysql://10.9.14.21:9030/online_disk?useUnicode=true&characterEncoding=UTF-8");
//        properties.put("load-url", "10.9.14.21:8030");
//        properties.put("username", "bigdata");
//        properties.put("password", "123456");
//        properties.put("sink.properties.format", "json");
//        properties.put("sink.properties.strip_outer_array", "true");
//        properties.put("sink.buffer-flush.interval-ms", "10000");
////        properties.put("sink.parallelism", "1");
//
//        SinkFunction<String> sink = StarRocksUtils.createStarRocksSinkFunction("db_test", "test_ods_biz_lead_star", properties);
//
//        stream.addSink(sink);
        env.execute();
    }
}

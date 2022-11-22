package com.byd;

import com.byd.schema.MySqlSchemaConverter;
import com.byd.schema.RecordInfo;
import com.byd.schema.RecordInfoDebeziumDeserializationSchema;
import com.byd.utils.CheckpointUtils;
import com.byd.functions.FilterAndAlarmRecordProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CDCTypeTest {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
//        config.setString("execution.savepoint.path", "file:///d:/ck/503c21577ebc1b08a6bf8a234984ee6d/chk-1");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        CheckpointUtils.setCheckpoint(env, "file:///d:/ck");
        DataStreamSource<RecordInfo> sourceStream = env.fromSource(createSource(args), WatermarkStrategy.noWatermarks(), "mysql");

        sourceStream.process(new FilterAndAlarmRecordProcessFunction() {
            @Override
            public void alarm(RecordInfo recordInfo) {
                System.out.println("数据库 schema change" + recordInfo);
            }
        });
        sourceStream.printToErr(">>>");
        env.execute();
    }

    public static MySqlSource<RecordInfo> createSource(String[] args) {
        MySqlSourceBuilder<RecordInfo> builder = MySqlSource.<RecordInfo>builder();
        builder.hostname("43.139.84.117")
                .port(3306)
                .scanNewlyAddedTableEnabled(true)
                .databaseList("test")
                .username("root")
                .password("123456")
                .tableList("test.stu")
                .startupOptions(StartupOptions.initial())
                .includeSchemaChanges(true)
                .serverId("5500-6400")
                .deserializer(new RecordInfoDebeziumDeserializationSchema(new MySqlSchemaConverter()));
        return builder.build();
    }
}

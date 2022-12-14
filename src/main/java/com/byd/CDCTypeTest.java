package com.byd;

import com.byd.schema.DefaultSchemaConverter;
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

import java.util.Properties;

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
        MySqlSourceBuilder<RecordInfo> builder = MySqlSource.builder();

        Properties prop = new Properties();
//        prop.setProperty("converters","isbn");
//        prop.setProperty("isbn.type", "com.byd.schema.MysqlTimeConverter");
        prop.setProperty("decimal.handling.mode","string");
        builder.hostname("10.17.7.197")
                .port(3306)
                .scanNewlyAddedTableEnabled(true)
                .databaseList("test")
                .username("canal")
                .password("Bigdata@123")
                .tableList("test.stu")
                .startupOptions(StartupOptions.initial())
                .includeSchemaChanges(true)
                .serverId("5400-5500")
                .chunkKeyColumn("id")
                .fetchSize(2048)
                .splitSize(1024)
                .serverId("5500-6400")
                .debeziumProperties(prop)
                .deserializer(new RecordInfoDebeziumDeserializationSchema(new DefaultSchemaConverter(true)));
        return builder.build();
    }
}

package com.byd.jobs;

import com.byd.schema.DefaultSchemaConverter;
import com.byd.schema.RecordInfo;
import com.byd.schema.RecordInfoDebeziumDeserializationSchema;
import com.byd.utils.CheckpointUtils;
import com.byd.functions.FilterAndAlarmRecordProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.writer.AbstractSingleOperationMapper;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.connector.writer.RowOperationMapper;
import org.apache.flink.connectors.kudu.streaming.KuduSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class CDCTypeTest {
    private static String kuduMaster = "master01-cdp03.byd.com:7051,master02-cdp03.byd.com:7051,master03-cdp03.byd.com:7051";

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
//        config.setString("execution.savepoint.path", "file:///d:/ck/503c21577ebc1b08a6bf8a234984ee6d/chk-1");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        CheckpointUtils.setCheckpoint(env, "file:///d:/ck");
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);


        DataStreamSource<RecordInfo> sourceStream = env.fromSource(createSource(args), WatermarkStrategy.noWatermarks(), "mysql");
        SingleOutputStreamOperator<Row> stream = sourceStream
                .process(new FilterAndAlarmRecordProcessFunction())
                .map(new MapFunction<RecordInfo, Row>() {
                    @Override
                    public Row map(RecordInfo value) throws Exception {
                        return value.getRow().toRow();
                    }
                });


        KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters(kuduMaster).build();

        KuduSink<Row> sink = new KuduSink<>(
                writerConfig,
                KuduTableInfo.forTable("impala::db_test.stu_test"),
                new RowOperationMapper(
                        new String[]{"id", "name", "age", "birthday", "ts", "money"},
                        AbstractSingleOperationMapper.KuduOperation.INSERT)
        );
        stream.addSink(sink);

        env.execute();
    }

    public static MySqlSource<RecordInfo> createSource(String[] args) {
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

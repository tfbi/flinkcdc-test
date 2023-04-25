package com.byd;

import com.byd.functions.FilterAndAlarmRecordProcessFunction;
import com.byd.kerberos.CommonConfig;
import com.byd.kerberos.KerberosAuth;
import com.byd.schema.*;
import com.byd.task.Task;
import com.byd.utils.CheckpointUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.writer.AbstractSingleOperationMapper;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.connector.writer.RowOperationMapper;
import org.apache.flink.connectors.kudu.streaming.KuduSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

public class Test {
    private static String kuduMaster = "master02-cdpdev-ic:7051,master03-cdpdev-ic:7051";

    public static void main(String[] args) throws Exception {
        CommonConfig.reload("config.properties");
        KerberosAuth.dealWithKbs(new Task() {
            @Override
            public Object execute() {
                runApp();
                return null;
            }
        });
    }

    private static void runApp() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        CheckpointUtils.setCheckpoint(env, "file:///d:/ck");
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        DataStreamSource<RecordInfo> sourceStream = env.fromSource(createSource(), WatermarkStrategy.noWatermarks(), "mysql");
        SingleOutputStreamOperator<TRow> stream = sourceStream
                .process(new FilterAndAlarmRecordProcessFunction())
                .map(new MapFunction<RecordInfo, TRow>() {
                    @Override
                    public TRow map(RecordInfo value) throws Exception {
                        return value.getRow();
                    }
                });


        KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters(kuduMaster).build();

        KuduSink<TRow> sink = new KuduSink<>(
                writerConfig,
                KuduTableInfo.forTable("impala::db_test.stu_test"),
                new TRowOperationMapper()
        );
        stream.addSink(sink);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
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

package com.byd;

import com.byd.schema.DefaultSchemaConverter;
import com.byd.schema.RecordInfo;
import com.byd.schema.RecordInfoDebeziumDeserializationSchema;
import com.byd.schema.TRow;
import com.byd.utils.CheckpointUtils;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.oracle.source.OracleSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import java.util.Properties;

public class OracleCdcTest {
    public static void main(String[] args) {
        Configuration config = new Configuration();
//        config.setString("execution.savepoint.path", "file:///d:/ck/535cdd97fe6fe4a86857fbbe6ad114ea/chk-2");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        OracleSourceBuilder<RecordInfo> builder = OracleSourceBuilder.OracleIncrementalSource.builder();

        CheckpointUtils.setCheckpoint(env, "file:///d:/ck");

        Properties prop = new Properties();
        prop.setProperty("log.mining.strategy", "online_catalog");
        prop.setProperty("log.mining.continuous.mine", "true");
//        prop.setProperty("converters","isbn");
//        prop.setProperty("isbn.type", "com.byd.schema.OracleTimeConverter");
//        prop.setProperty("time.precision.mode","connect");
        prop.setProperty("decimal.handling.mode", "string");

        OracleSourceBuilder.OracleIncrementalSource<RecordInfo> source = builder
                .url("jdbc:oracle:thin:@10.3.204.3:1521:orcl")
                .hostname("10.3.204.3")
                .port(1521)
                .databaseList("ORCL")
                .schemaList("FLINKUSER")
                .tableList("FLINKUSER.PERSON")
                .username("flinkuser")
                .password("flinkpw")
                .startupOptions(StartupOptions.initial())
                .includeSchemaChanges(true)
                .debeziumProperties(prop)
                .deserializer(new RecordInfoDebeziumDeserializationSchema(new DefaultSchemaConverter()))
                .build();
        DataStreamSource<RecordInfo> oracleStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "oracle");
        oracleStream.map(new MapFunction<RecordInfo, RowData>() {
            @Override
            public RowData map(RecordInfo value) throws Exception {
                return value.getRow().toRowData();
            }
        }).printToErr(">>>");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

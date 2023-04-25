package com.byd.jobs;

import com.byd.schema.DefaultSchemaConverter;
import com.byd.schema.RecordInfo;
import com.byd.schema.RecordInfoDebeziumDeserializationSchema;
import com.byd.utils.CheckpointUtils;
import com.ververica.cdc.connectors.oracle.source.OracleSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.util.Properties;

public class OracleCdcTest {
    public static void main(String[] args) {
        Configuration config = new Configuration();
//        config.setString("execution.savepoint.path", "file:///d:/ck/535cdd97fe6fe4a86857fbbe6ad114ea/chk-2");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
//        OracleSourceBuilder<RecordInfo> builder = OracleSourceBuilder.OracleIncrementalSource.builder();
        CheckpointUtils.setCheckpoint(env, "file:///d:/ck");

        Properties props = new Properties();
        props.setProperty("decimal.handling.mode", "string");
        props.setProperty("log.mining.continuous.mine", "true");
        props.setProperty("log.mining.strategy", "online_catalog");
        OracleSourceBuilder<RecordInfo> builder = OracleSourceBuilder.OracleIncrementalSource.builder();
        OracleSourceBuilder.OracleIncrementalSource<RecordInfo> oracleSource = builder.url("jdbc:oracle:thin:@10.9.28.187:1521:dms2").startupOptions(com.ververica.cdc.connectors.base.options.StartupOptions.initial()).username("flinkuser").password("flinkpwXbyd123").databaseList("dms").schemaList("DMS_OEM_PT").tableList("DMS_OEM_PT.TT_PT_ORDER_DETAIL").deserializer(new RecordInfoDebeziumDeserializationSchema(new DefaultSchemaConverter(true))).debeziumProperties(props).build();
        DataStreamSource<RecordInfo> sourceStream = env.fromSource(oracleSource, WatermarkStrategy.noWatermarks(), "oracle-cdc");
        sourceStream.map(new MapFunction<RecordInfo, String>() {
            @Override
            public String map(RecordInfo value) throws Exception {
                return value.getRow().toJsonWithKind();
            }
        }).printToErr(">>>");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

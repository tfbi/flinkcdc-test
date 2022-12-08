package com.byd;

import com.byd.schema.MySqlSchemaConverter;
import com.byd.schema.RecordInfo;
import com.byd.schema.RecordInfoDebeziumDeserializationSchema;
import com.byd.utils.CheckpointUtils;
import com.byd.utils.StreamUtils;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.oracle.source.OracleSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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

        OracleSourceBuilder.OracleIncrementalSource<RecordInfo> source = builder
                .url("jdbc:oracle:thin:@10.3.204.3:1521:orcl")
                .databaseList("ORCL")
                .schemaList("FLINKUSER")
                .tableList("FLINKUSER.STU")
                .username("flinkuser")
                .password("flinkpw")
                .startupOptions(StartupOptions.initial())
                .includeSchemaChanges(true)
                .chunkKeyColumn("s_id")
                .debeziumProperties(prop)
                .deserializer(new RecordInfoDebeziumDeserializationSchema(new MySqlSchemaConverter()))
                .build();
        DataStreamSource<RecordInfo> oracleStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "oracle");
        oracleStream.printToErr(">>>");
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

package com.byd;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.oracle.source.OracleSourceBuilder;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
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
        OracleSourceBuilder<String> builder = OracleSourceBuilder.OracleIncrementalSource.builder();

        Properties prop = new Properties();
        prop.setProperty("log.mining.strategy", "online_catalog");
        prop.setProperty("log.mining.continuous.mine", "true");

        OracleSourceBuilder.OracleIncrementalSource<String> source = builder
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
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> oracleStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "oracle");
        oracleStream.printToErr(">>>");
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

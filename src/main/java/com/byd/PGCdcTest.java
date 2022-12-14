package com.byd;

import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

public class PGCdcTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        Properties prop = new Properties();
        prop.setProperty("plugin.name", "pgoutput");
        SourceFunction<String> sourceFunction = PostgreSQLSource.<String>builder()
                .hostname("10.3.204.3")
                .port(5432)
                .database("postgres") // monitor postgres database
                .schemaList("db_test")  // monitor inventory schema
                .tableList("db_test.stu") // monitor products table
                .username("postgres")
                .password("123456")
                .debeziumProperties(prop)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();
        env
                .addSource(sourceFunction)
                .printToErr(">>>"); // use parallelism 1 for sink to keep message ordering

        env.execute();
    }
}

package com.byd;

import com.byd.schema.DefaultSchemaConverter;
import com.byd.schema.RecordInfo;
import com.byd.schema.RecordInfoDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

public class PGCdcTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        Properties prop = new Properties();
        prop.setProperty("plugin.name", "pgoutput");
        PostgreSQLSource.Builder<RecordInfo> builder = PostgreSQLSource.builder();
        SourceFunction<RecordInfo> sourceFunction = builder
                .hostname("10.3.204.3")
                .port(5432)
                .database("postgres") // monitor postgres database
                .schemaList("db_test")  // monitor inventory schema
                .tableList("db_test.stu") // monitor products table
                .username("postgres")
                .password("123456")
                .debeziumProperties(prop)
                .deserializer(new RecordInfoDebeziumDeserializationSchema(new DefaultSchemaConverter())) // converts SourceRecord to JSON String
                .build();
        DataStreamSource<RecordInfo> sourceStream = env
                .addSource(sourceFunction);

        sourceStream.printToErr(">>>");
        env.execute();
    }
}

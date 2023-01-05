package com.byd;

import com.byd.schema.DefaultSchemaConverter;
import com.byd.schema.RecordInfo;
import com.byd.schema.RecordInfoDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.oracle.source.OracleSourceBuilder;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

public class Test {
    public static void main(String[] args) throws Exception {
        runApp();
    }

    private static void runApp() throws java.lang.Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // OracleCdcReader;
        DataStreamSource<RecordInfo> oracleCdcReader_1 = oracleCdcReader_1(env);
        // PrintWriter;
        oracleCdcReader_1.print();
        // execute flink;
        env.execute();
    }



    private static DataStreamSource<RecordInfo> oracleCdcReader_1(StreamExecutionEnvironment env) {
        Properties props = new Properties();
        props.setProperty("plugin.name", "pgoutput");
        PostgreSQLSource.Builder<RecordInfo> builder = PostgreSQLSource.builder();
        SourceFunction<RecordInfo> postgresSource = builder.hostname("10.3.204.3").port(5432).username("postgres").password("123456").database("postgres").schemaList("db_test").tableList("db_test.stu").deserializer(new RecordInfoDebeziumDeserializationSchema(new DefaultSchemaConverter())).debeziumProperties(props).build();
        DataStreamSource<RecordInfo> sourceStream = env.addSource(postgresSource);
        return sourceStream;
    }
}

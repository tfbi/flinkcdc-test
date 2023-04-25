package com.byd.jobs;

import com.byd.functions.ShuntStreamProcessFunction;
import com.byd.schema.DefaultSchemaConverter;
import com.byd.schema.RecordInfo;
import com.byd.schema.RecordInfoDebeziumDeserializationSchema;
import com.byd.utils.StarRocksUtils;
import com.byd.utils.StreamUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class CatalogTest {
    public static void main(String[] args) throws Exception {
        runApp();
    }

    private static void runApp() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        // CheckpointSetter;
        checkpointSetter_0(env);
        // MysqlCdcReader;
        DataStreamSource<RecordInfo> mysqlCdcReader_1 = mysqlCdcReader_1(env);
        Map<String, OutputTag<RecordInfo>> outputTagMap = createOutputTagMap();
        // ShuntCDCStream;
        SingleOutputStreamOperator<RecordInfo> shuntCDCStream_2 = mysqlCdcReader_1.process(new ShuntStreamProcessFunction("db.table", outputTagMap));
        // StarRocksWriter;
        Map<String, SinkFunction<String>> starRocksSinkMap = createStarRocksSinkMap();
        StreamUtils.streamSinkToStarRocks(shuntCDCStream_2, outputTagMap, starRocksSinkMap);
        // execute flink;
        env.execute();
    }

    private static void checkpointSetter_0(StreamExecutionEnvironment env) {
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(60), CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://master02-cdpdev-ic:8020/tmp/flink_ck/");
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(10));
//        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
//        env.getCheckpointConfig().setCheckpointTimeout(TimeUnit.SECONDS.toMillis(120));
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    }

    private static DataStreamSource<RecordInfo> mysqlCdcReader_1(StreamExecutionEnvironment env) {
        MySqlSourceBuilder<RecordInfo> builder = MySqlSource.builder();
        Properties prop = new Properties();
        prop.setProperty("decimal.handling.mode", "string");
        MySqlSource<RecordInfo> mysqlSource = builder.startupOptions(com.ververica.cdc.connectors.mysql.table.StartupOptions.initial()).serverId("5400-5500").hostname("10.17.7.197").port(3306).username("canal").password("Bigdata@123").debeziumProperties(prop).databaseList("test").tableList("test.stu", "test.person").includeSchemaChanges(true).deserializer(new RecordInfoDebeziumDeserializationSchema(new DefaultSchemaConverter(true))).build();
        DataStreamSource<RecordInfo> sourceStream = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysql-cdc");
        return sourceStream;
    }

    private static Map<String, OutputTag<RecordInfo>> createOutputTagMap() {
        Map<String, OutputTag<RecordInfo>> outputTagMap = new HashMap<String, OutputTag<RecordInfo>>();
        outputTagMap.put("test.stu", new OutputTag<RecordInfo>("test.stu") {
        });
        outputTagMap.put("test.person", new OutputTag<RecordInfo>("test.person") {
        });
        return outputTagMap;
    }

    private static Map<String, SinkFunction<String>> createStarRocksSinkMap() throws Exception {
        Map<String, String> tableMap = new HashMap<String, String>();
        tableMap.put("stu", "stu");
        tableMap.put("person", "person");
        Map<String, String> starRocksProperties = new HashMap<String, String>();
        starRocksProperties.put("jdbc-url", "jdbc:mysql://10.17.16.77:9030/db_test?useUnicode=true&characterEncoding=UTF-8");
        starRocksProperties.put("password", "123456");
        starRocksProperties.put("load-url", "10.17.16.77:8030");
        starRocksProperties.put("sink.properties.format", "json");
        starRocksProperties.put("sink.properties.strip_outer_array", "true");
        starRocksProperties.put("sink.parallelism", "1");
        starRocksProperties.put("username", "bigdata");
        Map<String, SinkFunction<String>> starRocksSinkMap = StarRocksUtils.createStarRocksSinkMap("test", "db_test", tableMap, starRocksProperties);
        return starRocksSinkMap;
    }
}

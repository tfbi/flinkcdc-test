package com.byd.jobs;

import com.byd.functions.ShuntStreamProcessFunction;
import com.byd.schema.DefaultSchemaConverter;
import com.byd.schema.RecordInfo;
import com.byd.schema.RecordInfoDebeziumDeserializationSchema;
import com.byd.utils.StarRocksUtils;
import com.byd.utils.StreamUtils;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class PG2StarRocks {

    public static void main(String[] args) {
        try {
            runApp();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void runApp() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // CheckpointSetter;
        checkpointSetter_0(env);
        // PostgresqlCdcReader;
        DataStreamSource<RecordInfo> postgresqlCdcReader_1 = postgresqlCdcReader_1(env);
        Map<String, OutputTag<RecordInfo>> outputTagMap = createOutputTagMap();
        // ShuntCDCStream;
        SingleOutputStreamOperator<RecordInfo> shuntCDCStream_2 = postgresqlCdcReader_1.process(new ShuntStreamProcessFunction("schema.table", outputTagMap));
        // StarRocksWriter;
        Map<String, SinkFunction<String>> starRocksSinkMap = createStarRocksSinkMap();
        StreamUtils.streamSinkToStarRocks(shuntCDCStream_2, outputTagMap, starRocksSinkMap);
        // execute flink;
        env.execute();
    }

    private static void checkpointSetter_0(StreamExecutionEnvironment env) {
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(600), CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ck");
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(10));
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(100);
        env.getCheckpointConfig().setCheckpointTimeout(TimeUnit.SECONDS.toMillis(120));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    }

    private static DataStreamSource<RecordInfo> postgresqlCdcReader_1(StreamExecutionEnvironment env) {
        Properties props = new Properties();
        props.setProperty("plugin.name", "pgoutput");
        props.setProperty("decimal.handling.mode", "string");
        props.setProperty("slot.drop.on.stop", "true");
        PostgreSQLSource.Builder<RecordInfo> builder = PostgreSQLSource.builder();
        SourceFunction<RecordInfo> postgresSource = builder.hostname("10.17.11.77").port(5432).username("ods_user").password("123#byd").database("online-disk-master").schemaList("odm_user").tableList("odm_user.app_api", "odm_user.favorites", "odm_user.file_collect", "odm_user.file_user", "odm_user.i_share", "odm_user.notification", "odm_user.notification_user", "odm_user.operate", "odm_user.operationlog", "odm_user.permission", "odm_user.permission_operate", "odm_user.project", "odm_user.project_team", "odm_user.project_user_permission", "odm_user.recovery_file", "odm_user.storage", "odm_user.sys_user", "odm_user.sysparam", "odm_user.team", "odm_user.team_user").deserializer(new RecordInfoDebeziumDeserializationSchema(new DefaultSchemaConverter(true))).debeziumProperties(props).build();
        DataStreamSource<RecordInfo> sourceStream = env.addSource(postgresSource);
        return sourceStream;
    }

    private static Map<String, OutputTag<RecordInfo>> createOutputTagMap() {
        Map<String, OutputTag<RecordInfo>> outputTagMap = new HashMap<String, OutputTag<RecordInfo>>();
        outputTagMap.put("odm_user.app_api", new OutputTag<RecordInfo>("odm_user.app_api") {
        });
        outputTagMap.put("odm_user.favorites", new OutputTag<RecordInfo>("odm_user.favorites") {
        });
        outputTagMap.put("odm_user.file_collect", new OutputTag<RecordInfo>("odm_user.file_collect") {
        });
        outputTagMap.put("odm_user.file_user", new OutputTag<RecordInfo>("odm_user.file_user") {
        });
        outputTagMap.put("odm_user.i_share", new OutputTag<RecordInfo>("odm_user.i_share") {
        });
        outputTagMap.put("odm_user.notification", new OutputTag<RecordInfo>("odm_user.notification") {
        });
        outputTagMap.put("odm_user.notification_user", new OutputTag<RecordInfo>("odm_user.notification_user") {
        });
        outputTagMap.put("odm_user.operate", new OutputTag<RecordInfo>("odm_user.operate") {
        });
        outputTagMap.put("odm_user.operationlog", new OutputTag<RecordInfo>("odm_user.operationlog") {
        });
        outputTagMap.put("odm_user.permission", new OutputTag<RecordInfo>("odm_user.permission") {
        });
        outputTagMap.put("odm_user.permission_operate", new OutputTag<RecordInfo>("odm_user.permission_operate") {
        });
        outputTagMap.put("odm_user.project", new OutputTag<RecordInfo>("odm_user.project") {
        });
        outputTagMap.put("odm_user.project_team", new OutputTag<RecordInfo>("odm_user.project_team") {
        });
        outputTagMap.put("odm_user.project_user_permission", new OutputTag<RecordInfo>("odm_user.project_user_permission") {
        });
        outputTagMap.put("odm_user.recovery_file", new OutputTag<RecordInfo>("odm_user.recovery_file") {
        });
        outputTagMap.put("odm_user.storage", new OutputTag<RecordInfo>("odm_user.storage") {
        });
        outputTagMap.put("odm_user.sysparam", new OutputTag<RecordInfo>("odm_user.sysparam") {
        });
        outputTagMap.put("odm_user.team", new OutputTag<RecordInfo>("odm_user.team") {
        });
        outputTagMap.put("odm_user.team_user", new OutputTag<RecordInfo>("odm_user.team_user") {
        });
        return outputTagMap;
    }

    private static Map<String, SinkFunction<String>> createStarRocksSinkMap() throws Exception {
        Map<String, String> tableMap = new HashMap<String, String>();
        tableMap.put("favorites", "favorites");
        tableMap.put("project_team", "project_team");
        tableMap.put("recovery_file", "recovery_file");
        tableMap.put("file_user", "file_user");
        tableMap.put("project", "project");
        tableMap.put("permission", "permission");
        tableMap.put("team_user", "team_user");
        tableMap.put("storage", "storage");
        tableMap.put("team", "team");
        tableMap.put("app_api", "app_api");
        tableMap.put("sysparam", "sysparam");
        tableMap.put("file_collect", "file_collect");
        tableMap.put("notification", "notification");
        tableMap.put("notification_user", "notification_user");
        tableMap.put("project_user_permission", "project_user_permission");
        tableMap.put("operate", "operate");
        tableMap.put("operationlog", "operationlog");
        tableMap.put("i_share", "i_share");
        tableMap.put("permission_operate", "permission_operate");
        Map<String, String> starRocksProperties = new HashMap<String, String>();
        starRocksProperties.put("jdbc-url", "jdbc:mysql://10.9.14.21:9030/db_test?useUnicode=true&characterEncoding=UTF-8");
        starRocksProperties.put("password", "123456");
        starRocksProperties.put("load-url", "10.9.14.21:8030");
        starRocksProperties.put("sink.properties.format", "json");
        starRocksProperties.put("sink.properties.strip_outer_array", "true");
        starRocksProperties.put("sink.parallelism", "1");
        starRocksProperties.put("username", "bigdata");
        Map<String, SinkFunction<String>> starRocksSinkMap = StarRocksUtils.createStarRocksSinkMap("odm_user", "online_disk", tableMap, starRocksProperties);
        return starRocksSinkMap;
    }
}

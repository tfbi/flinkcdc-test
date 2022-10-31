package com.byd;

import com.byd.schema.TableRow;
import com.byd.schema.TableRowDataDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CDCJob {
    public static void main(String[] args) throws Exception {
        try {
            runAPP();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void runAPP() throws Exception {
        // flink env
//        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration = new Configuration();
        configuration.setString("execution.savepoint.path", "file:///D://ck/12ea46b863ff9d00121047dda8cf9989/chk-62");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///d://ck");
        env.getCheckpointConfig().
                enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //debezium config
//        Properties prop = new Properties();
//        prop.put("decimal.handling.mode", "string");
//
//        Map<String, Object> customConverterConfigs = new HashMap<String, Object>();
//        customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());

        // mysql-cdc
        MySqlSource<TableRow> source = MySqlSource.<TableRow>builder()
                .hostname("43.139.84.117")
                .port(3306)
                .scanNewlyAddedTableEnabled(true)
                .databaseList("test") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList("test.stu") // 设置捕获的表
                .username("root")
                .password("123456")
                //.debeziumProperties(prop)
                .startupOptions(StartupOptions.initial())
                .deserializer(new TableRowDataDebeziumDeserializationSchema())
                .build();

        SingleOutputStreamOperator<RowData> mysqlStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .map((MapFunction<TableRow, RowData>) TableRow::getRowData);
        // hudi sink

        mysqlStream.printToErr(">>>");

        String targetTable = "t1";
        String basePath = "file:///d://hudi/t1";

        Map<String, String> options = new HashMap<>();
        options.put(FlinkOptions.PATH.key(), basePath);
        options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
//        options.put(FlinkOptions.PRECOMBINE_FIELD.key(), "ts");

        HoodiePipeline.Builder builder = HoodiePipeline.builder(targetTable)
                .column("id BIGINT")
                .column("name STRING")
                .column("age INT")
                .column("birthday DATE")
                .column("ts TIMESTAMP(3)")
                .column("money DECIMAL(16,2)")
                .column("`partition` STRING")
                .pk("id")
                .partition("partition")
                .options(options);

        builder.sink(mysqlStream, false); // The second parameter indicating whether the input data stream is bounded
        env.execute("Api_Sink");
    }
}

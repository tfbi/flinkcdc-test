package com.byd;

import com.byd.schema.MySqlSchemaConverter;
import com.byd.schema.TableRowData;
import com.byd.schema.TableRowDataDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

import java.util.HashMap;
import java.util.Map;

public class Cdc2HudiJob {
    public static void main(String[] args) throws Exception {
        try {
            runAPP();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void runAPP() throws Exception {
        // flink env
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///d://ck");
        env.getCheckpointConfig().
                enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        /* debezium config
         Properties prop = new Properties();
         prop.put("decimal.handling.mode", "string");
         Map<String, Object> customConverterConfigs = new HashMap<String, Object>();
         customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
         */

        // db-cdc
        MySqlSource<TableRowData> source = MySqlSource.<TableRowData>builder()
                .hostname("43.139.84.117")
                .port(3306)
                .scanNewlyAddedTableEnabled(true)
                .databaseList("test")
                .tableList("test.stu")
                .username("root")
                .password("123456")
                .startupOptions(StartupOptions.initial())
                .deserializer(new TableRowDataDebeziumDeserializationSchema(new MySqlSchemaConverter()))
                .build();

        SingleOutputStreamOperator<RowData> mysqlStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .map((MapFunction<TableRowData, RowData>) TableRowData::getRowData);

        mysqlStream.printToErr(">>>");

        // hudi sink
        String targetTable = "t1";
        String basePath = "file:///d://hudi/t1";
        Map<String, String> options = new HashMap<>();
        options.put(FlinkOptions.PATH.key(), basePath);
        options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
        options.put(FlinkOptions.PRECOMBINE_FIELD.key(), "ts");

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

        builder.sink(mysqlStream, false);
        env.execute("Api_Sink");
    }
}

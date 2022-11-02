package com.byd.utils;

import com.byd.schema.MySqlSchemaConverter;
import com.byd.schema.TableRowData;
import com.byd.schema.TableRowDataDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

/**
 * @author bi.tengfei1
 */
public class CDCUtils {
    public static final ConfigOption<String> HOST_NAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .defaultValue("localhost");


    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(3306);


    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .defaultValue("root");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .defaultValue("123456");

    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("数据库名称，已锁定为单库");

    public static final ConfigOption<String> TABLE_LIST_STR =
            ConfigOptions.key("table_list_str")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("表名称列表，用逗号分割");

    public static MySqlSource<TableRowData> createMysqlCDCSource(Configuration config) {
        String tableListStr = config.get(TABLE_LIST_STR);
        String database = config.get(DATABASE);
        MySqlSourceBuilder<TableRowData> builder = MySqlSource.<TableRowData>builder();
        String[] tableArray = tableListStr.split(",");
        for (int i = 0; i < tableArray.length; i++) {
            tableArray[i] = database + "." + tableArray[i];
        }
        builder.hostname(config.get(HOST_NAME))
                .port(config.get(PORT))
                .databaseList(config.get(DATABASE))
                .username(config.get(USERNAME))
                .password(config.get(PASSWORD))
                .tableList(tableArray)
                .startupOptions(StartupOptions.initial())
                .deserializer(new TableRowDataDebeziumDeserializationSchema(new MySqlSchemaConverter()));

        return builder.build();
    }
}

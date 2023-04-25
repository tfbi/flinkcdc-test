package com.byd.kerberos;

import com.byd.utils.ConfigUtils;
import com.byd.utils.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author wang.yonggui1
 * @date 2021-12-29 11:46
 */
public enum CommonConfig implements Config{



    /**
     * constant
     */
    JAVA_SECURITY_AUTH_LOGIN_CONFIG("java.security.auth.login.config","java.security.auth.login.config",""),
    SASL_PLAINTEXT("sasl_plaintext","SASL_PLAINTEXT","kafka ssl"),
    GSSAPI("GSSAPI","GSSAPI",""),
    KAFKA("kafka","kafka",""),

    /**
     * table generator
     */
    TG_SYS_SHORTNAME("tg.sys.shortname","test",""),
    TG_HIVE_DEFAULT_PARTITION_STRATEGY("tg.hive.default.partition.strategy","",""),
    TG_SOURCE_USERNAME("tg.source.username","",""),
    TG_SOURCE_PASSWORD("tg.source.password","",""),
    TG_SOURCE_DRIVER("tg.source.driver","",""),
    TG_SOURCE_URL("tg.source.url","",""),
    TG_SINK_KERBEROS_ON("tg.sink.kerberos.on","1",""),
    TG_SINK_AUTO_DDL("tg.sink.auto.ddl","1",""),
    TG_SINK_URL("tg.sink.url","",""),
    TG_SINK_USERNAME("tg.sink.username","",""),
    TG_SINK_PASSWORD("tg.sink.password","",""),
    TG_SINK_DRIVER("tg.sink.driver","",""),
    TG_SYS_INCRE_FIELD("tg.sys.incre_field","",""),
    TG_KUDU_DEFAULT_RANGE_PARTITION("tg.kudu.default.range.partition.value","",""),
    TG_TABLES("tg.tables","",""),
    TG_SOURCE_TYPE("tg.source.type","",""),
    TG_SINK_TYPE("tg.sink.type","",""),
    TG_KUDU_DEFAULT_HASH_PARTITIONS("tg.kudu.default.hash.partitions","4",""),
    TG_KUDU_DEFAULT_RANGE_PARTITION_NAME("tg.kudu.default.range.partition.name","",""),
    TG_KUDU_DEFAULT_RANGE_PARTITION_COMMENT("tg.kudu.default.range.partition.comment","",""),
    TG_KUDU_DEFAULT_RANGE_PARTITION_VALUE("tg.kudu.default.range.partition.value","",""),
    TG_SINK_RANGE_PARTITION_ON("tg.sink.range.partition.on","0",""),
    TG_SOURCE_TABLE_SCHEMA("tg.source.table.schema","",""),

    /**
     * cdc & flink & kafka
     */
    SOURCE_TYPE_NAME("source.type.name","canal_mysql",""),
    FLINK_RUN_MODE("flink.run.mode","0","0-local,1-on yarn"),
    CHECK_PONINT_PATH("flink.checkpoint.path","hdfs://master02-cdpdev-ic:8020/data/flink/checkpoint","hdfs"),
    BOOTSTRAPS_ERVER("flink.kafka.bootstrap_server","worker01-cdpdev-ic:9092,worker02-cdpdev-ic:9092,worker03-cdpdev-ic:9092",""),
    KAFKA_GROUP("flink.kafka.group","test_group",""),
    KAFKA_TOPIC("flink.kafka.topic","",""),
    FLINK_PARALLELISM("flink.parallelism","1",""),
    KAFKA_JASSFILE("flink.kafka.jaas_file","kerberos/jaas.conf",""),

    /**
     * keberos
     */
    KERBEROS_FLUSH_INTERVAL("tg.sink.kerberos.flush.interval","120","min"),
    KERBEROS_PRINCIPAL("tg.sink.kerberos.principal","ic.dev.hdfs@BYD.CN",""),
    KERBEROS_KRB5_CONF_PATH("tg.sink.kerberos.krb5.conf.path","kerberos/krb5.conf",""),
    KERBEROS_KEYTAB_PATH("tg.sink.kerberos.keytab.path","",""),
    HADOOP_CONF_DIR("tg.sink.hadoop.conf.dir","hadoop",""),
    KERBEROS_KRBHOSTFQDN("tg.sink.kerberos.KrbHostFQDN","",""),
    KERBEROS_KRBSERVICENAME("tg.sink.kerberos.KrbServiceName","",""),

    /**
     * kudu
     */
    KUDU_UPSERT_BATCH_TIMEOUT("kudu.upsert.batch.timeout","10000","time out to upsert(ms)"),
    KUDU_MASTER("kudu.masters","master02-cdpdev-ic:7051,master03-cdpdev-ic:7051",""),
    KUDU_TABLE_PREFIX("kudu.table.prefix","impala::db_test.",""),
    KUDU_TABLE_SUFFIX("kudu.table.suffix","",""),
    KUDU_LOGIC_DELETE_ON("kudu.logic.delete.on","0","logic delete: 0-off 1-on"),
    KUDU_LOGIC_DELETE_FIELD("kudu.logic.delete.field","is_delete_cdc",""),
    KUDU_TENANT_ON("kudu.tenant.on","0","## tenant range partition: 0-off 1-on"),
    KUDU_TENANT_KEY("kudu.tenant.key","key",""),
    KUDU_TENANT_VALUE("kudu.tenant.value","value",""),
    KUDU_AUTO_DDL("kudu.auto.ddl","0","## auto to ddl for kudu: 0-off 1-on"),

    /**
     * spark
     */
    SPARK_RUN_MODE("spark.run.mode","1","0-local 1-yarn")



    /**
     * ALARM TYPE
     */
//    DDL_ALTER("alarm.ddl.alter", "5", "数据库alter操作"),
//    DML_DELETE("alarm.dml.delete", "5", "数据库删除操作"),
//    EXCEPTION_ALARM("alarm.exception","5","异常告警"),
//
//    WXHOOK_URL("alarm.wxhook.url","https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=02f8719a-3f9e-40d2-95f1-2b0261e8635b",""),
    ;

    private String key;
    private String value;
    private String desc;
    private static Map<String,String> PROPERTIES=new HashMap<>(8);


    CommonConfig(String key, String value, String des) {
        this.key = key;
        this.value = value;
        this.desc = des;
    }

    /**
     * reload properties
     * @param filePath
     */
    public static synchronized void reload(String filePath){
        PROPERTIES= ConfigUtils.prop2Map(ConfigUtils.loadProperties(filePath));
    }

    /**
     * reload2
     * @param props
     */
    public static synchronized void reload(Logger log,Map<String,String> props){
        if (null!=PROPERTIES){
            PROPERTIES.clear();
        }
        PROPERTIES.putAll(props);
        log.info("reload properties at {}", DateUtils.getDateTime());
    }

    public static synchronized void reload(Map<String,String> props){
        if (null!=PROPERTIES){
            PROPERTIES.clear();
        }
        PROPERTIES.putAll(props);
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        String strBykey = PROPERTIES.get(key);
        if (!StringUtils.isEmpty(strBykey)){
            return strBykey;
        }
        return value;
    }

    @Override
    public String getDesc() {
        return desc;
    }


    public Integer getValueInt() {
        Integer strBykey = ConfigUtils.getIntBykey(PROPERTIES, key);
        if (null!=strBykey){
            return strBykey;
        }
        return Integer.parseInt(value);
    }

    public void setValue(String value) {
        this.value = value;
    }



    public static void main(String[] args) {
        CommonConfig.reload("config-local.properties");
        System.out.println(CommonConfig.FLINK_RUN_MODE.getValueInt());
    }



}

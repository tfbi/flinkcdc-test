package com.byd.schema;

import com.ververica.cdc.debezium.table.DeserializationRuntimeConverter;
import org.apache.kafka.connect.data.Schema;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseSchemaConverter implements Serializable {
    protected final ZoneId serverTimeZone;
    protected static Map<String, DeserializationRuntimeConverter> classConverterMap = new HashMap<>();
    protected static Map<Schema.Type, DeserializationRuntimeConverter> typeConverterMap = new HashMap<>();

    protected BaseSchemaConverter(ZoneId serverTimeZone) {
        this.serverTimeZone = serverTimeZone;
        intConvertMap();
    }
    protected BaseSchemaConverter() {
        this.serverTimeZone = ZoneId.of("Asia/Shanghai");
        intConvertMap();
    }

    protected abstract void intConvertMap();

    public abstract Object convert(Schema.Type type, Object obj, Schema schema) throws Exception;

    public abstract Object convert(String schemaName, Object obj, Schema schema) throws Exception;
}

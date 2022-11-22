package com.byd.schema;

import org.apache.flink.table.types.DataType;
import org.apache.kafka.connect.data.Schema;
import java.io.Serializable;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseSchemaConverter implements Serializable {
    public final ZoneId serverTimeZone;
    public Map<String, DeserializationConverter> classConverterMap;
    public Map<Schema.Type, DeserializationConverter> typeConverterMap;

    public BaseSchemaConverter(ZoneId serverTimeZone) {
        this.serverTimeZone = serverTimeZone;
        classConverterMap = new HashMap<>();
        typeConverterMap = new HashMap<>();
    }

    protected BaseSchemaConverter() {
        this(ZoneId.of("Asia/Shanghai"));
    }

    public abstract void intConvertMap();


    public Object convert(Schema.Type type, Object obj, Schema schema) throws Exception {
        return typeConverterMap.get(type).convert(obj, schema);
    }

    public Object convert(String schemaName, Object obj, Schema schema) throws Exception {
        return classConverterMap.get(schemaName).convert(obj, schema);
    }


    public DataType getDataType(Schema.Type type) throws Exception {
        return typeConverterMap.get(type).getDataType();
    }

    public DataType getDataType(String schemaName) throws Exception {
        return classConverterMap.get(schemaName).getDataType();
    }
}

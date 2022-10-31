package com.byd.schema;

import com.ververica.cdc.debezium.table.DeserializationRuntimeConverter;
import io.debezium.time.*;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;

import java.time.ZoneId;


import static com.byd.schema.ConvertFunctions.*;

public class MySqlSchemaConverter extends BaseConverter {

    public MySqlSchemaConverter(ZoneId serverTimeZone) {
        super(serverTimeZone);
    }

    public MySqlSchemaConverter() {
        super();
    }

    @Override
    protected void intConvertMap() {
        final DeserializationRuntimeConverter timestampConvert = convertToTimestamp(serverTimeZone);
        classConverterMap.put(Timestamp.SCHEMA_NAME, timestampConvert);
        classConverterMap.put(MicroTimestamp.SCHEMA_NAME, timestampConvert);
        classConverterMap.put(NanoTimestamp.SCHEMA_NAME, timestampConvert);
        classConverterMap.put(org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME, timestampConvert);

        final DeserializationRuntimeConverter timeConvert = convertToTime();
        classConverterMap.put(MicroTime.SCHEMA_NAME, timeConvert);
        classConverterMap.put(NanoTime.SCHEMA_NAME, timeConvert);
        classConverterMap.put(org.apache.kafka.connect.data.Time.LOGICAL_NAME, timeConvert);

        DeserializationRuntimeConverter dateConvert = convertToDate();
        classConverterMap.put(org.apache.kafka.connect.data.Date.LOGICAL_NAME, dateConvert);
        classConverterMap.put(Date.SCHEMA_NAME, dateConvert);

        DeserializationRuntimeConverter decimalConverter = createDecimalConverter();
        classConverterMap.put(Decimal.LOGICAL_NAME, decimalConverter);
        DeserializationRuntimeConverter localTimeZoneConvert = convertToLocalTimeZoneTimestamp();

        classConverterMap.put(ZonedTimestamp.SCHEMA_NAME, convertToLocalTimeZoneTimestamp());


        DeserializationRuntimeConverter intConvert = convertToInt();
        typeConverterMap.put(Schema.Type.INT16, intConvert);
        typeConverterMap.put(Schema.Type.INT32, intConvert);

        DeserializationRuntimeConverter longConvert = convertToLong();
        typeConverterMap.put(Schema.Type.INT64, longConvert);

        DeserializationRuntimeConverter floatConvert = convertToFloat();
        typeConverterMap.put(Schema.Type.FLOAT32, floatConvert);

        DeserializationRuntimeConverter doubleConvert = convertToDouble();
        typeConverterMap.put(Schema.Type.FLOAT64, doubleConvert);

        DeserializationRuntimeConverter boolConvert = convertToBoolean();
        typeConverterMap.put(Schema.Type.BOOLEAN, boolConvert);

        DeserializationRuntimeConverter bytesConvert = convertToBinary();
        typeConverterMap.put(Schema.Type.BYTES, bytesConvert);

        DeserializationRuntimeConverter stringConvert = convertToString();
        typeConverterMap.put(Schema.Type.STRING, stringConvert);

    }

    @Override
    public Object convert(Schema.Type type, Object obj, Schema schema) throws Exception {
        return typeConverterMap.get(type).convert(obj, schema);
    }

    @Override
    public Object convert(String schemaName, Object obj, Schema schema) throws Exception {
        return classConverterMap.get(schemaName).convert(obj, schema);
    }
}

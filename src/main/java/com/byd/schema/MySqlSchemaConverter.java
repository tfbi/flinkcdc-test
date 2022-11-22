package com.byd.schema;

import io.debezium.time.*;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import java.time.ZoneId;



import static com.byd.schema.ConvertFunctions.*;

public class MySqlSchemaConverter extends BaseSchemaConverter {


    public MySqlSchemaConverter(ZoneId serverTimeZone) {
        super(serverTimeZone);
        intConvertMap();
    }

    public MySqlSchemaConverter() {
        super();
        intConvertMap();
    }

    @Override
    public void intConvertMap() {
        final DeserializationConverter timestampConvert = convertToTimestamp(serverTimeZone);
        classConverterMap.put(Timestamp.SCHEMA_NAME, timestampConvert);
        classConverterMap.put(MicroTimestamp.SCHEMA_NAME, timestampConvert);
        classConverterMap.put(NanoTimestamp.SCHEMA_NAME, timestampConvert);
        classConverterMap.put(org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME, timestampConvert);

        final DeserializationConverter timeConvert = convertToTime();
        classConverterMap.put(MicroTime.SCHEMA_NAME, timeConvert);
        classConverterMap.put(NanoTime.SCHEMA_NAME, timeConvert);
        classConverterMap.put(org.apache.kafka.connect.data.Time.LOGICAL_NAME, timeConvert);

        DeserializationConverter dateConvert = convertToDate();
        classConverterMap.put(org.apache.kafka.connect.data.Date.LOGICAL_NAME, dateConvert);
        classConverterMap.put(Date.SCHEMA_NAME, dateConvert);

        DeserializationConverter decimalConverter = createDecimalConverter();
        classConverterMap.put(Decimal.LOGICAL_NAME, decimalConverter);

        DeserializationConverter localTimeZoneConvert = convertToLocalTimeZoneTimestamp();
        classConverterMap.put(ZonedTimestamp.SCHEMA_NAME, localTimeZoneConvert);


        DeserializationConverter intConvert = convertToInt();
        typeConverterMap.put(Schema.Type.INT16, intConvert);
        typeConverterMap.put(Schema.Type.INT32, intConvert);

        DeserializationConverter longConvert = convertToLong();
        typeConverterMap.put(Schema.Type.INT64, longConvert);

        DeserializationConverter floatConvert = convertToFloat();
        typeConverterMap.put(Schema.Type.FLOAT32, floatConvert);

        DeserializationConverter doubleConvert = convertToDouble();
        typeConverterMap.put(Schema.Type.FLOAT64, doubleConvert);

        DeserializationConverter boolConvert = convertToBoolean();
        typeConverterMap.put(Schema.Type.BOOLEAN, boolConvert);

        DeserializationConverter bytesConvert = convertToBinary();
        typeConverterMap.put(Schema.Type.BYTES, bytesConvert);

        DeserializationConverter stringConvert = convertToString();
        typeConverterMap.put(Schema.Type.STRING, stringConvert);

    }
}

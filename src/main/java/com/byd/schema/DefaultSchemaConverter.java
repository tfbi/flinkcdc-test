package com.byd.schema;

import io.debezium.time.*;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;

import java.time.ZoneId;

public class DefaultSchemaConverter extends BaseSchemaConverter {


    public DefaultSchemaConverter(ZoneId serverTimeZone, boolean isEnableTimeToString) {
        super(serverTimeZone, isEnableTimeToString);
        intConvertMap();
    }

    public DefaultSchemaConverter(boolean isEnableTimeToString) {
        super(isEnableTimeToString);
        intConvertMap();
    }

    public DefaultSchemaConverter() {
        super(false);
        intConvertMap();
    }

    @Override
    public void intConvertMap() {
        DeserializationConverter timestampConvert = BaseConvertFunctions.convertToTimestamp(serverTimeZone);
        if (isEnableTimeToString) {
            timestampConvert = DateForStringConvertFunctions.convertToTimestamp(serverTimeZone);
        }
        classConverterMap.put(Timestamp.SCHEMA_NAME, timestampConvert);
        classConverterMap.put(MicroTimestamp.SCHEMA_NAME, timestampConvert);
        classConverterMap.put(NanoTimestamp.SCHEMA_NAME, timestampConvert);
        classConverterMap.put(org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME, timestampConvert);


        DeserializationConverter timeConvert = BaseConvertFunctions.convertToTime();
        if (isEnableTimeToString) {
            timeConvert = DateForStringConvertFunctions.convertToTime();
        }
        classConverterMap.put(MicroTime.SCHEMA_NAME, timeConvert);
        classConverterMap.put(NanoTime.SCHEMA_NAME, timeConvert);
        classConverterMap.put(org.apache.kafka.connect.data.Time.LOGICAL_NAME, timeConvert);

        DeserializationConverter dateConvert = BaseConvertFunctions.convertToDate();
        if (isEnableTimeToString) {
            dateConvert = DateForStringConvertFunctions.convertToDate();
        }
        classConverterMap.put(org.apache.kafka.connect.data.Date.LOGICAL_NAME, dateConvert);
        classConverterMap.put(Date.SCHEMA_NAME, dateConvert);

        DeserializationConverter localTimeZoneConvert = BaseConvertFunctions.convertToLocalTimeZoneTimestamp(serverTimeZone);
        if (isEnableTimeToString) {
            localTimeZoneConvert = DateForStringConvertFunctions.convertToLocalTimeZoneTimestamp(serverTimeZone);
        }
        classConverterMap.put(ZonedTimestamp.SCHEMA_NAME, localTimeZoneConvert);


        DeserializationConverter decimalConverter = BaseConvertFunctions.createDecimalConverter();
        classConverterMap.put(Decimal.LOGICAL_NAME, decimalConverter);

        DeserializationConverter intConvert = BaseConvertFunctions.convertToInt();
        typeConverterMap.put(Schema.Type.INT16, intConvert);
        typeConverterMap.put(Schema.Type.INT32, intConvert);

        DeserializationConverter longConvert = BaseConvertFunctions.convertToLong();
        typeConverterMap.put(Schema.Type.INT64, longConvert);

        DeserializationConverter floatConvert = BaseConvertFunctions.convertToFloat();
        typeConverterMap.put(Schema.Type.FLOAT32, floatConvert);

        DeserializationConverter doubleConvert = BaseConvertFunctions.convertToDouble();
        typeConverterMap.put(Schema.Type.FLOAT64, doubleConvert);

        DeserializationConverter boolConvert = BaseConvertFunctions.convertToBoolean();
        typeConverterMap.put(Schema.Type.BOOLEAN, boolConvert);

        DeserializationConverter bytesConvert = BaseConvertFunctions.convertToBinary();
        typeConverterMap.put(Schema.Type.BYTES, bytesConvert);

        DeserializationConverter stringConvert = BaseConvertFunctions.convertToString();
        typeConverterMap.put(Schema.Type.STRING, stringConvert);

    }
}

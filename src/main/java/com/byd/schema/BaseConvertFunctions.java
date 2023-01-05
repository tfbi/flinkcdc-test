package com.byd.schema;

import com.ververica.cdc.debezium.utils.TemporalConversions;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.time.*;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Map;

public class BaseConvertFunctions {


    public static DeserializationConverter convertToBoolean() {
        return new DeserializationConverter() {

            @Override
            public DataType getDataType() {
                return DataTypes.BOOLEAN();
            }

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Boolean) {
                    return dbzObj;
                } else if (dbzObj instanceof Byte) {
                    return (byte) dbzObj == 1;
                } else if (dbzObj instanceof Short) {
                    return (short) dbzObj == 1;
                } else {
                    return Boolean.parseBoolean(dbzObj.toString());
                }
            }
        };
    }

    public static DeserializationConverter convertToInt() {
        return new DeserializationConverter() {

            @Override
            public DataType getDataType() {
                return DataTypes.INT();
            }

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Integer) {
                    return dbzObj;
                } else if (dbzObj instanceof Long) {
                    return ((Long) dbzObj).intValue();
                } else {
                    return Integer.parseInt(dbzObj.toString());
                }
            }
        };
    }

    public static DeserializationConverter convertToLong() {
        return new DeserializationConverter() {

            @Override
            public DataType getDataType() {
                return DataTypes.BIGINT();
            }

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Integer) {
                    return ((Integer) dbzObj).longValue();
                } else if (dbzObj instanceof Long) {
                    return dbzObj;
                } else {
                    return Long.parseLong(dbzObj.toString());
                }
            }
        };
    }

    public static DeserializationConverter convertToDouble() {
        return new DeserializationConverter() {

            @Override
            public DataType getDataType() {
                return DataTypes.DOUBLE();
            }

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Float) {
                    return ((Float) dbzObj).doubleValue();
                } else if (dbzObj instanceof Double) {
                    return dbzObj;
                } else {
                    return Double.parseDouble(dbzObj.toString());
                }
            }
        };
    }

    public static DeserializationConverter convertToFloat() {
        return new DeserializationConverter() {

            @Override
            public DataType getDataType() {
                return DataTypes.FLOAT();
            }

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Float) {
                    return dbzObj;
                } else if (dbzObj instanceof Double) {
                    return ((Double) dbzObj).floatValue();
                } else {
                    return Float.parseFloat(dbzObj.toString());
                }
            }
        };
    }

    public static DeserializationConverter convertToDate() {

        return new DeserializationConverter() {

            @Override
            public DataType getDataType() {
                return DataTypes.INT();
            }

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                return (int) TemporalConversions.toLocalDate(dbzObj).toEpochDay();
            }
        };
    }

    public static DeserializationConverter convertToTime() {
        return new DeserializationConverter() {


            @Override
            public DataType getDataType() {
                return DataTypes.INT();
            }

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Long) {
                    switch (schema.name()) {
                        case MicroTime.SCHEMA_NAME:
                            return (int) ((long) dbzObj / 1000);
                        case NanoTime.SCHEMA_NAME:
                            return (int) ((long) dbzObj / 1000_000);
                    }
                } else if (dbzObj instanceof Integer) {
                    return dbzObj;
                }
                // get number of milliseconds of the day
                return TemporalConversions.toLocalTime(dbzObj).toSecondOfDay() * 1000;
            }
        };
    }

    public static DeserializationConverter convertToTimestamp(ZoneId serverTimeZone) {

        return new DeserializationConverter() {

            @Override
            public DataType getDataType() {
                return DataTypes.TIMESTAMP(3).bridgedTo(TimestampData.class);
            }

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Long) {
                    switch (schema.name()) {
                        case Timestamp.SCHEMA_NAME:
                            return TimestampData.fromEpochMillis((Long) dbzObj);
                        case MicroTimestamp.SCHEMA_NAME:
                            long micro = (long) dbzObj;
                            return TimestampData.fromEpochMillis(
                                    micro / 1000, (int) (micro % 1000 * 1000));
                        case NanoTimestamp.SCHEMA_NAME:
                            long nano = (long) dbzObj;
                            return TimestampData.fromEpochMillis(
                                    nano / 1000_000, (int) (nano % 1000_000));
                    }
                }
                LocalDateTime localDateTime =
                        TemporalConversions.toLocalDateTime(dbzObj, serverTimeZone);
                return TimestampData.fromLocalDateTime(localDateTime);
            }
        };
    }

    public static DeserializationConverter convertToLocalTimeZoneTimestamp(ZoneId serverTimeZone) {
        return new DeserializationConverter() {
            private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");

            @Override
            public DataType getDataType() {
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).bridgedTo(TimestampData.class);
            }

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) throws ParseException {
                if (dbzObj instanceof String) {
                    String str = (String) dbzObj;
                    if (str.contains("+08:00")) {
                        Date date = sdf.parse(str);
                        return TimestampData.fromLocalDateTime(
                                LocalDateTime.ofInstant(date.toInstant(), serverTimeZone));
                    }
                    // TIMESTAMP_LTZ type is encoded in string type
                    Instant instant = Instant.parse(str);
                    return TimestampData.fromLocalDateTime(
                            LocalDateTime.ofInstant(instant, serverTimeZone));
                }
                throw new IllegalArgumentException(
                        "Unable to convert to TimestampData from unexpected value '"
                                + dbzObj
                                + "' of type "
                                + dbzObj.getClass().getName());
            }
        };
    }

    public static DeserializationConverter convertToString() {

        return new DeserializationConverter() {

            @Override
            public DataType getDataType() {
                return DataTypes.STRING().bridgedTo(StringData.class);
            }

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                return StringData.fromString(dbzObj.toString());
            }
        };
    }

    public static DeserializationConverter convertToBinary() {
        return new DeserializationConverter() {

            @Override
            public DataType getDataType() {
                return DataTypes.BYTES();
            }

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof byte[]) {
                    return dbzObj;
                } else if (dbzObj instanceof ByteBuffer) {
                    ByteBuffer byteBuffer = (ByteBuffer) dbzObj;
                    byte[] bytes = new byte[byteBuffer.remaining()];
                    byteBuffer.get(bytes);
                    return bytes;
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported BYTES value type: " + dbzObj.getClass().getSimpleName());
                }
            }
        };
    }

    public static DeserializationConverter createDecimalConverter() {
        return new DeserializationConverter() {

            @Override
            public DataType getDataType() {
                return DataTypes.DECIMAL(precision, scale).bridgedTo(DecimalData.class);
            }

            private static final long serialVersionUID = 1L;
            private int precision = DecimalType.DEFAULT_PRECISION;
            private int scale = DecimalType.DEFAULT_PRECISION;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                Map<String, String> parameters = schema.parameters();
                if (parameters.containsKey("scale")) {
                    scale = Integer.parseInt(parameters.get("scale"));
                }
                if (parameters.containsKey("connect.decimal.precision")) {
                    precision = Integer.parseInt(parameters.get("connect.decimal.precision"));
                }
                BigDecimal bigDecimal;
                if (dbzObj instanceof byte[]) {
                    // decimal.handling.mode=precise
                    bigDecimal = Decimal.toLogical(schema, (byte[]) dbzObj);
                } else if (dbzObj instanceof String) {
                    // decimal.handling.mode=string
                    bigDecimal = new BigDecimal((String) dbzObj);
                } else if (dbzObj instanceof Double) {
                    // decimal.handling.mode=double
                    bigDecimal = BigDecimal.valueOf((Double) dbzObj);
                } else {
                    if (VariableScaleDecimal.LOGICAL_NAME.equals(schema.name())) {
                        SpecialValueDecimal decimal =
                                VariableScaleDecimal.toLogical((Struct) dbzObj);
                        bigDecimal = decimal.getDecimalValue().orElse(BigDecimal.ZERO);
                    } else {
                        // fallback to string
                        bigDecimal = new BigDecimal(dbzObj.toString());
                    }
                }
                return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
            }
        };
    }
}

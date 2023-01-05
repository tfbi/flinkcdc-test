package com.byd.schema;

import com.byd.utils.DateUtils;
import com.ververica.cdc.debezium.utils.TemporalConversions;
import io.debezium.time.*;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.kafka.connect.data.Schema;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

public class DateForStringConvertFunctions {

    private static final long MS_8HOUR = 8 * 60 * 60 * 1000;
    private static final long MS_24HOUR = 24 * 60 * 60 * 1000;

    public static DeserializationConverter convertToDate() {

        return new DeserializationConverter() {

            @Override
            public DataType getDataType() {
                return DataTypes.STRING().bridgedTo(StringData.class);
            }

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                long ret = TemporalConversions.toLocalDate(dbzObj).toEpochDay() * MS_24HOUR;
                return DateUtils.formatDate(new Date(ret));
            }
        };
    }

    public static DeserializationConverter convertToTime() {
        return new DeserializationConverter() {

            @Override
            public DataType getDataType() {
                return DataTypes.STRING().bridgedTo(StringData.class);
            }

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                int ret;
                if (dbzObj instanceof Long) {
                    switch (schema.name()) {
                        case MicroTime.SCHEMA_NAME:
                            ret = (int) ((long) dbzObj / 1000);
                        case NanoTime.SCHEMA_NAME:
                            ret = (int) ((long) dbzObj / 1000_000);
                    }
                } else if (dbzObj instanceof Integer) {
                    ret = (int) dbzObj;
                }
                // get number of milliseconds of the day
                ret = TemporalConversions.toLocalTime(dbzObj).toSecondOfDay() * 1000;
                return DateUtils.formatDate(new Date(ret - MS_8HOUR));
            }
        };
    }

    public static DeserializationConverter convertToTimestamp(ZoneId serverTimeZone) {

        return new DeserializationConverter() {

            @Override
            public DataType getDataType() {
                return DataTypes.STRING().bridgedTo(StringData.class);
            }

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                TimestampData ret = null;
                if (dbzObj instanceof Long) {
                    switch (schema.name()) {
                        case Timestamp.SCHEMA_NAME:
                            ret = TimestampData.fromEpochMillis((Long) dbzObj);
                            break;
                        case MicroTimestamp.SCHEMA_NAME:
                            long micro = (long) dbzObj;
                            ret = TimestampData.fromEpochMillis(
                                    micro / 1000, (int) (micro % 1000 * 1000));
                            break;
                        case NanoTimestamp.SCHEMA_NAME:
                            long nano = (long) dbzObj;
                            ret = TimestampData.fromEpochMillis(
                                    nano / 1000_000, (int) (nano % 1000_000));
                            break;
                    }
                } else {
                    LocalDateTime localDateTime =
                            TemporalConversions.toLocalDateTime(dbzObj, serverTimeZone);
                    ret = TimestampData.fromLocalDateTime(localDateTime);
                }
                assert ret != null;
                if (ret.getMillisecond() % MS_24HOUR == 0) {
                    return DateUtils.formatDate(new Date(ret.getMillisecond()));
                }
                return DateUtils.formatDateTime(new Date(ret.getMillisecond() - MS_8HOUR));
            }
        };
    }

    public static DeserializationConverter convertToLocalTimeZoneTimestamp(ZoneId serverTimeZone) {
        return new DeserializationConverter() {
            private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");

            @Override
            public DataType getDataType() {
                return DataTypes.STRING().bridgedTo(StringData.class);
            }

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) throws ParseException {
                TimestampData ret = null;
                if (dbzObj instanceof String) {
                    String str = (String) dbzObj;
                    if (str.contains("+08:00")) {
                        Date date = sdf.parse(str);
                        ret = TimestampData.fromLocalDateTime(
                                LocalDateTime.ofInstant(date.toInstant(), serverTimeZone));
                    } else {
                        // TIMESTAMP_LTZ type is encoded in string type
                        Instant instant = Instant.parse(str);
                        ret = TimestampData.fromLocalDateTime(
                                LocalDateTime.ofInstant(instant, serverTimeZone));
                    }
                    return DateUtils.formatTimestampMs(ret.getMillisecond() - MS_8HOUR);
                }
                throw new IllegalArgumentException(
                        "Unable to convert to TimestampData from unexpected value '"
                                + dbzObj
                                + "' of type "
                                + dbzObj.getClass().getName());
            }
        };
    }
}

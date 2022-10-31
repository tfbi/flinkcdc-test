/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.byd.schema;

import com.ververica.cdc.debezium.table.DeserializationRuntimeConverter;
import com.ververica.cdc.debezium.utils.TemporalConversions;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.time.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Deserialization schema from Debezium object to Flink Table/SQL internal data structure {@link
 * RowData}.
 */
public final class TableRowDataDebeziumDeserializationSchema
        implements DebeziumDeserializationSchema<TableRow> {

    private static final String SOURCE = "source";
    private static final String TABLE = "table";
    private final ZoneId serverTimeZone;


    private static Map<String, DeserializationRuntimeConverter> classConverterMap = new HashMap<>();
    private static Map<Schema.Type, DeserializationRuntimeConverter> typeConverterMap = new HashMap<>();


    public TableRowDataDebeziumDeserializationSchema(ZoneId serverTimeZone) {
        this.serverTimeZone = serverTimeZone;
        initConverterMap();
    }

    public TableRowDataDebeziumDeserializationSchema() {
        this.serverTimeZone = ZoneId.of("Asia/Shanghai");
        initConverterMap();
    }

    private void initConverterMap() {

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
    public void deserialize(SourceRecord record, Collector<TableRow> out) throws Exception {
        Envelope.Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        // get source table
        final Struct source = value.getStruct(SOURCE);
        String table = source.getString(TABLE);
        // judge rowKind
        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            GenericRowData insert = extractAfterRow(value, valueSchema);
            insert.setRowKind(RowKind.INSERT);
            emit(insert, table, out);
        } else if (op == Envelope.Operation.DELETE) {
            GenericRowData delete = extractBeforeRow(value, valueSchema);
            delete.setRowKind(RowKind.DELETE);
            emit(delete, table, out);
        } else {
            GenericRowData before = extractBeforeRow(value, valueSchema);
            before.setRowKind(RowKind.UPDATE_BEFORE);
            emit(before, table, out);

            GenericRowData after = extractAfterRow(value, valueSchema);
            after.setRowKind(RowKind.UPDATE_AFTER);
            emit(after, table, out);
        }
    }

    private GenericRowData extractAfterRow(Struct value, Schema valueSchema) throws Exception {
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        return extractRow(after, valueSchema);
    }

    public GenericRowData extractRow(Struct value, Schema valueSchema) {
        System.out.println(valueSchema);
        List<Field> fields = value.schema().fields();
        GenericRowData rowData = new GenericRowData(fields.size() + 1);
        int pos = 0;
        for (Field field : fields) {
            String schemaName = field.schema().name();
            Object fieldValue = null;
            if (schemaName != null && classConverterMap.containsKey(schemaName)) {
                try {
                    fieldValue = classConverterMap.get(schemaName).convert(value.get(field), field.schema());
                } catch (Exception e) {
                    e.printStackTrace();
                    fieldValue = value.get(field);
                }
            } else {
                try {
                    Schema.Type type = field.schema().type();
                    fieldValue = typeConverterMap.get(type).convert(value.get(field), field.schema());
                } catch (Exception e) {
                    e.printStackTrace();
                    fieldValue = value.get(field);
                }
            }
            rowData.setField(pos++, fieldValue);
        }
        // partition
        try {
            rowData.setField(pos, typeConverterMap.get(Schema.Type.STRING).convert("part1", null));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rowData;
    }

    private GenericRowData extractBeforeRow(Struct value, Schema valueSchema) throws Exception {
        Struct before = value.getStruct(Envelope.FieldName.BEFORE);
        return extractRow(before, valueSchema);
    }

    private void emit(RowData rowData, String sourceTable, Collector<TableRow> collector) {
        collector.collect(TableRow.builder().rowData(rowData).sourceTable(sourceTable).build());
    }

    @Override
    public TypeInformation<TableRow> getProducedType() {
        return BasicTypeInfo.of(TableRow.class);
    }


    private static DeserializationRuntimeConverter convertToBoolean() {
        return new DeserializationRuntimeConverter() {

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

    private static DeserializationRuntimeConverter convertToInt() {
        return new DeserializationRuntimeConverter() {

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

    private static DeserializationRuntimeConverter convertToLong() {
        return new DeserializationRuntimeConverter() {

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

    private static DeserializationRuntimeConverter convertToDouble() {
        return new DeserializationRuntimeConverter() {

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

    private static DeserializationRuntimeConverter convertToFloat() {
        return new DeserializationRuntimeConverter() {

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

    private static DeserializationRuntimeConverter convertToDate() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                return (int) TemporalConversions.toLocalDate(dbzObj).toEpochDay();
            }
        };
    }

    private static DeserializationRuntimeConverter convertToTime() {
        return new DeserializationRuntimeConverter() {

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

    private static DeserializationRuntimeConverter convertToTimestamp(ZoneId serverTimeZone) {
        return new DeserializationRuntimeConverter() {

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

    private static DeserializationRuntimeConverter convertToLocalTimeZoneTimestamp() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof String) {
                    String str = (String) dbzObj;
                    // TIMESTAMP_LTZ type is encoded in string type
                    Instant instant = Instant.parse(str);
                    return TimestampData.fromLocalDateTime(
                            LocalDateTime.ofInstant(instant, ZoneId.of("UTC")));
                }
                throw new IllegalArgumentException(
                        "Unable to convert to TimestampData from unexpected value '"
                                + dbzObj
                                + "' of type "
                                + dbzObj.getClass().getName());
            }
        };
    }

    private static DeserializationRuntimeConverter convertToString() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                return StringData.fromString(dbzObj.toString());
            }
        };
    }

    private static DeserializationRuntimeConverter convertToBinary() {
        return new DeserializationRuntimeConverter() {

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

    private static DeserializationRuntimeConverter createDecimalConverter() {
        return new DeserializationRuntimeConverter() {

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

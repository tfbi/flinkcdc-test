package com.byd.schema;

import com.google.gson.Gson;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.RowUtils;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.*;

public class TRow implements Serializable {
    private static final long serialVersionUID = 3L;

    private RowKind kind;

    private final Object[] fieldByPosition;

    private final LinkedHashMap<String, Integer> positionByName;

    private final DataType[] fieldTypeByPosition;
    private final String[] fieldNameByPosition;

    TRow(
            RowKind kind,
            Object[] fieldByPosition,
            LinkedHashMap<String, Integer> positionByName,
            DataType[] fieldTypePosition, String[] fieldNameByPosition) {
        this.kind = kind;
        this.fieldByPosition = fieldByPosition;
        this.positionByName = positionByName;
        this.fieldTypeByPosition = fieldTypePosition;
        this.fieldNameByPosition = fieldNameByPosition;
    }


    public TRow(RowKind kind, int arity) {
        this.kind = Preconditions.checkNotNull(kind, "Row kind must not be null.");
        this.fieldByPosition = new Object[arity];
        this.fieldTypeByPosition = new DataType[arity];
        this.fieldNameByPosition = new String[arity];
        this.positionByName = new LinkedHashMap<>();
    }


    public TRow(int arity) {
        this(RowKind.INSERT, arity);
    }


    public RowKind getKind() {
        return kind;
    }


    public void setKind(RowKind kind) {
        Preconditions.checkNotNull(kind, "Row kind must not be null.");
        this.kind = kind;
    }


    public int getArity() {
        return fieldByPosition.length;
    }


    public Object getField(int pos) {
        if (pos >= 0 && pos < fieldByPosition.length) {
            return fieldByPosition[pos];
        } else {
            throw new IndexOutOfBoundsException(
                    String.format("index out of Array: %s.", pos));
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T getFieldAs(int pos) {
        return (T) getField(pos);
    }

    public Object getField(String name) {
        if (positionByName.containsKey(name)) {
            int pos = positionByName.get(name);
            return getField(pos);
        } else {
            throw new IllegalArgumentException(String.format("no field name:%s in Trow.", name));
        }
    }

    public DataType getFieldType(String name) {
        if (positionByName.containsKey(name)) {
            int pos = positionByName.get(name);
            return getFieldType(pos);
        } else {
            throw new IllegalArgumentException(String.format("no field name:%s in Trow.", name));
        }
    }

    public DataType getFieldType(int pos) {
        if (pos >= 0 && pos < fieldTypeByPosition.length) {
            return fieldTypeByPosition[pos];
        } else {
            throw new IndexOutOfBoundsException(
                    String.format("index out of Array: %s", pos));
        }
    }

    public String getFieldName(int pos) {
        if (pos >= 0 && pos < fieldNameByPosition.length) {
            return fieldNameByPosition[pos];
        } else {
            throw new IndexOutOfBoundsException(
                    String.format("index out of Array: %s", pos));
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T getFieldAs(String name) {
        return (T) getField(name);
    }


    public void setField(int pos, String name, Object value) {
        setField(pos, name, value, null);
    }

    public void reSetField(String name, Object value) {
        if (positionByName.containsKey(name)) {
            int pos = positionByName.get(name);
            setField(pos, name, value);
        } else {
            throw new IllegalArgumentException(String.format("no field name:%s in Trow", name));
        }
    }


    public void setField(int pos, String name, Object value, DataType fieldType) {
        if (pos >= 0 && pos < fieldByPosition.length) {
            positionByName.put(name, pos);
            fieldByPosition[pos] = value;
            fieldTypeByPosition[pos] = fieldType;
            fieldNameByPosition[pos] = name;
        } else {
            throw new IndexOutOfBoundsException(
                    String.format("index out of Array: %s", pos));
        }
    }

    public void setFieldType(int pos, DataType fieldType) {
        if (pos >= 0 && pos < fieldTypeByPosition.length) {
            fieldTypeByPosition[pos] = fieldType;
        } else {
            throw new IndexOutOfBoundsException(
                    String.format("index out of Array: %s", pos));
        }
    }

    public void setFieldType(String name, DataType dataType) {
        if (positionByName.containsKey(name)) {
            int pos = positionByName.get(name);
            setFieldType(pos, dataType);
        } else {
            throw new IllegalArgumentException(String.format("no field name:%s in Trow", name));
        }
    }
    public void clear() {
        if (fieldByPosition != null) {
            Arrays.fill(fieldByPosition, null);
        }
        if (fieldTypeByPosition != null) {
            Arrays.fill(fieldTypeByPosition, null);
        }
        if (fieldNameByPosition != null) {
            Arrays.fill(fieldNameByPosition, null);
        }
        positionByName.clear();
    }

    @Override
    public String toString() {
        return "TRow{" +
                "kind=" + kind +
                ", fieldByPosition=" + Arrays.toString(fieldByPosition) +
                ", positionByName=" + positionByName +
                ", fieldTypeByPosition=" + Arrays.toString(fieldTypeByPosition) +
                ", fieldNameByPosition=" + Arrays.toString(fieldNameByPosition) +
                '}';
    }

    public Row toRow() {
        return RowUtils.createRowWithNamedPositions(kind, fieldByPosition, positionByName);
    }

    public RowData toRowData() {
        int arity = fieldByPosition.length;
        GenericRowData rowData = new GenericRowData(kind, arity);
        for (int pos = 0; pos < arity; pos++) {
            if (getFieldType(pos).equals(DataTypes.STRING())) {
                rowData.setField(pos, StringData.fromString((String) fieldByPosition[pos]));
            } else {
                rowData.setField(pos, fieldByPosition[pos]);
            }
        }
        return rowData;
    }

    public RowData toRowDataWithPartition(Object partition) {
        int arity = fieldByPosition.length;
        GenericRowData rowData = new GenericRowData(kind, arity + 1);

        for (int pos = 0; pos < arity; pos++) {
            if (getFieldType(pos).equals(DataTypes.STRING())) {
                rowData.setField(pos, StringData.fromString((String) fieldByPosition[pos]));
            } else {
                rowData.setField(pos, fieldByPosition[pos]);
            }
        }
        rowData.setField(rowData.getArity() - 1, partition);
        return rowData;
    }

    public String toJsonWithKind() {
        Map<String, Object> data = new HashMap<>();
        for (String fieldName : positionByName.keySet()) {
            data.put(fieldName, getField(fieldName));
        }
        if (kind == RowKind.DELETE) {
            data.put("__op", "1");
        } else {
            data.put("__op", "0");
        }
        return new Gson().toJson(data);
    }
}

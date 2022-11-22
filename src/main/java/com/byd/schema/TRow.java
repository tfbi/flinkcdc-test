package com.byd.schema;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
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

    TRow(
            RowKind kind,
            Object[] fieldByPosition,
            LinkedHashMap<String, Integer> positionByName,
            DataType[] fieldTypePosition) {
        this.kind = kind;
        this.fieldByPosition = fieldByPosition;
        this.positionByName = positionByName;
        this.fieldTypeByPosition = fieldTypePosition;
    }


    public TRow(RowKind kind, int arity) {
        this.kind = Preconditions.checkNotNull(kind, "Row kind must not be null.");
        this.fieldByPosition = new Object[arity];
        this.fieldTypeByPosition = new DataType[arity];
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

    public DataType getFileType(String name) {
        if (positionByName.containsKey(name)) {
            int pos = positionByName.get(name);
            return getFieldType(pos);
        } else {
            throw new IllegalArgumentException(String.format("no field name:%s in Trow.", name));
        }
    }

    private DataType getFieldType(int pos) {
        if (pos >= 0 && pos < fieldTypeByPosition.length) {
            return fieldTypeByPosition[pos];
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
        positionByName.clear();
    }

    @Override
    public String toString() {
        return "TRow{" +
                "kind=" + kind +
                ", fieldByPosition=" + Arrays.toString(fieldByPosition) +
                ", fieldTypeByPosition=" + Arrays.toString(fieldTypeByPosition) +
                '}';
    }

    public Row toRow() {
        return RowUtils.createRowWithNamedPositions(kind, fieldByPosition, positionByName);
    }

    public RowData toRowData() {
        int arity = fieldByPosition.length;
        GenericRowData rowData = new GenericRowData(kind, arity);
        for (int i = 0; i < arity; i++) {
            rowData.setField(i, fieldByPosition[i]);
        }
        return rowData;
    }

    public RowData toRowDataWithPartition(Object partition) {
        int arity = fieldByPosition.length;
        GenericRowData rowData = new GenericRowData(kind, arity + 1);

        for (int pos = 0; pos < arity; pos++) {
            rowData.setField(pos, fieldByPosition[pos]);
        }
        rowData.setField(rowData.getArity() - 1, partition);
        return rowData;
    }
}

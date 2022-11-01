package com.byd.schema;


import lombok.Builder;
import lombok.Data;
import org.apache.flink.table.data.RowData;


import java.io.Serializable;

@Data
@Builder
public class TableRowData implements Serializable {
    private String sourceTable;
    private RowData rowData;
}

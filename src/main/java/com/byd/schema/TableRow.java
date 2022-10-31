package com.byd.schema;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.table.data.RowData;


import java.io.Serializable;

/**
 * 提取出 [table、rowKind、rowKing 对应结果的 json] 序列化结果
 */
@Data
@Builder
public class TableRow implements Serializable {
    private String sourceTable;
    private RowData rowData;
}

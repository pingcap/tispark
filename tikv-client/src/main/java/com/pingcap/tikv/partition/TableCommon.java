package com.pingcap.tikv.partition;

import com.pingcap.tikv.meta.TiTableInfo;
import lombok.Data;

@Data
public class TableCommon {

  private final long logicalTableId;

  private final long physicalTableId;

  private final TiTableInfo tableInfo;
}

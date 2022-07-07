package com.pingcap.tikv.partition;

import com.pingcap.tikv.meta.TiTableInfo;
import java.io.Serializable;
import lombok.Data;

@Data
public class TableCommon implements Serializable {

  private final long logicalTableId;

  private final long physicalTableId;

  private final TiTableInfo tableInfo;
}

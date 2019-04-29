package com.pingcap.tikv;


import com.pingcap.tikv.catalog.CatalogTransaction;

public class AutoIDGenerator {
  private long start;
  private long end;
  private boolean signed;
  private long dbId;
  private CatalogTransaction catalogTransaction;

  public AutoIDGenerator(long dbId, CatalogTransaction catalogTransaction) {
    this.catalogTransaction = catalogTransaction;
    this.dbId = dbId;
  }

  public boolean isSigned() {
    return signed;
  }

  public long alloc(long tableId, long step) {
    if(isSigned()) {
      return allocSigned(tableId, step);
    }
    return allocUnSigned(tableId, step);
  }

  private long allocSigned(long tableId, long step) {
    long newEnd;
    if(start == end) {
      // get new start from tikv, and calculate new end and set it back to tikv.
      long newStart = allocID(dbId, tableId);
      long tmpStep = Math.min(Long.MAX_VALUE - newStart, step);
      newEnd  = allocID(dbId, tableId, tmpStep);
      if(start == Long.MAX_VALUE) {
        // TODO: refine this expcetion
        throw new IllegalArgumentException("cannot allocate more ids since it ")
      }
      end = newEnd;
    }

    return start++;
  }

  private long allocUnSigned(long tableId, long step) {
//    long newEnd;
//    if(start == end) {
//       get new start from tikv, and calculate new end and set it back to tikv.
//      long newStart = catalogTransaction.getAutoTableId(dbId, tableId);
//      long tmpStep = Math.min(Long.MAX_VALUE - newStart, step);
//      newEnd  = allocID(dbId, tableId, tmpStep);
//      if(start == Long.MAX_VALUE) {
//         TODO: throw new exception indicates we can't allocate any more id for now.
//      }
//      end = newEnd;
    return 0L;
//    }

//    return start++;
  }

  private long allocID(long dbId, long tableId) {
    return catalogTransaction.getAutoTableId(dbId, tableId);
  }

  private long allocID(long dbId, long tableId, long step) {
    return catalogTransaction.getAutoTableId(dbId, tableId, step);
  }
}

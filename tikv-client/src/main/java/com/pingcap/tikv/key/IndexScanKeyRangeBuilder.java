package com.pingcap.tikv.key;

import static com.pingcap.tikv.util.KeyRangeUtils.makeCoprocRange;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.predicates.IndexRange;
import org.tikv.kvproto.Coprocessor.KeyRange;

public class IndexScanKeyRangeBuilder {
  private final long id;
  private final TiIndexInfo index;
  private final IndexRange ir;
  private final Key pointKey;
  private Key lPointKey;
  private Key uPointKey;
  private Key lKey;
  private Key uKey;

  public IndexScanKeyRangeBuilder(long id, TiIndexInfo index, IndexRange ir) {
    this.id = id;
    this.index = index;
    this.ir = ir;
    pointKey = ir.hasAccessKey() ? ir.getAccessKey() : Key.EMPTY;
  }

  private KeyRange computeWithOutRange() {
    lPointKey = pointKey;
    uPointKey = pointKey.nextPrefix();

    lKey = Key.EMPTY;
    uKey = Key.EMPTY;
    return toPairKey();
  }

  private KeyRange toPairKey() {
    IndexKey lbsKey = IndexKey.toIndexKey(id, index.getId(), lPointKey, lKey);
    IndexKey ubsKey = IndexKey.toIndexKey(id, index.getId(), uPointKey, uKey);
    return makeCoprocRange(lbsKey.toByteString(), ubsKey.toByteString());
  }

  private KeyRange computeWithRange() {
    Range<TypedKey> range = ir.getRange();
    lPointKey = pointKey;
    uPointKey = pointKey;

    if (!range.hasLowerBound()) {
      // -INF
      lKey = Key.MIN;
    } else {
      lKey = range.lowerEndpoint();
      if (range.lowerBoundType().equals(BoundType.OPEN)) {
        lKey = lKey.next();
      }
    }

    if (!range.hasUpperBound()) {
      // INF
      uKey = Key.MAX;
    } else {
      uKey = range.upperEndpoint();
      if (range.upperBoundType().equals(BoundType.CLOSED)) {
        uKey = uKey.next();
      }
    }
    return toPairKey();
  }

  public KeyRange compute() {
    if (!ir.hasRange()) {
      return computeWithOutRange();
    } else {
      return computeWithRange();
    }
  }
}

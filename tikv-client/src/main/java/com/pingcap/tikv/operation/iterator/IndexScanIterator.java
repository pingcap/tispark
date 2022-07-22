/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.operation.iterator;

import com.pingcap.tikv.ClientSession;
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.handle.Handle;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.util.RangeSplitter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorCompletionService;
import org.tikv.common.util.RangeSplitter.RegionTask;

public class IndexScanIterator implements Iterator<Row> {
  private final Iterator<Handle> handleIterator;
  private final TiDAGRequest dagReq;
  private final Snapshot snapshot;
  private final ExecutorCompletionService<Iterator<Row>> completionService;
  private final int batchSize;
  private Iterator<Row> rowIterator;
  private int batchCount = 0;

  public IndexScanIterator(Snapshot snapshot, TiDAGRequest req, Iterator<Handle> handleIterator) {
    ClientSession clientSession = snapshot.getClientSession();
    TiConfiguration conf = clientSession.getConf();
    this.dagReq = req;
    this.handleIterator = handleIterator;
    this.snapshot = snapshot;
    this.batchSize = conf.getIndexScanBatchSize();
    this.completionService =
        new ExecutorCompletionService<>(clientSession.getTikvSession().getThreadPoolForIndexScan());
  }

  private List<Handle> feedBatch() {
    List<Handle> handles = new ArrayList<>(512);
    while (handleIterator.hasNext()) {
      handles.add(handleIterator.next());
      if (batchSize <= handles.size()) {
        break;
      }
    }
    return handles;
  }

  @Override
  public boolean hasNext() {
    try {
      if (rowIterator == null) {
        ClientSession clientSession = snapshot.getClientSession();
        while (handleIterator.hasNext()) {
          List<Handle> handles = feedBatch();
          batchCount++;
          completionService.submit(
              () -> {
                List<RegionTask> tasks = new ArrayList<>();
                List<Long> ids = dagReq.getPrunedPhysicalIds();
                tasks.addAll(
                    RangeSplitter.newSplitter(clientSession.getTikvSession().getRegionManager())
                        .splitAndSortHandlesByRegion(ids, handles));

                return CoprocessorIterator.getRowIterator(dagReq, tasks, clientSession);
              });
        }
        while (batchCount > 0) {
          rowIterator = completionService.take().get();
          batchCount--;

          if (rowIterator.hasNext()) {
            return true;
          }
        }
      }
      if (rowIterator == null) {
        return false;
      }
    } catch (Exception e) {
      throw new TiClientInternalException("Error reading rows from handle", e);
    }
    return rowIterator.hasNext();
  }

  @Override
  public Row next() {
    if (hasNext()) {
      return rowIterator.next();
    } else {
      throw new NoSuchElementException();
    }
  }
}

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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.streaming;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import org.tikv.kvproto.Coprocessor;
import org.tikv.kvproto.Errorpb;

public class StreamingResponse implements Iterable {
  private final Iterator<Coprocessor.Response> resultIterator;
  private final List<Coprocessor.Response> responseList;

  @SuppressWarnings("unchecked")
  public StreamingResponse(Iterator resultIterator) {
    requireNonNull(resultIterator, "Streaming result cannot be null!");
    this.resultIterator = resultIterator;
    responseList = new ArrayList<>();
    fetchStreamingResult();
  }

  private void fetchStreamingResult() {
    while (resultIterator.hasNext()) {
      responseList.add(resultIterator.next());
    }
  }

  public boolean hasRegionError() {
    for (Coprocessor.Response response : responseList) {
      if (response.hasRegionError()) {
        return true;
      }
    }

    return false;
  }

  public Errorpb.Error getFirstRegionError() {
    for (Coprocessor.Response response : responseList) {
      if (response.hasRegionError()) {
        return response.getRegionError();
      }
    }
    return null;
  }

  public String getFirstOtherError() {
    for (Coprocessor.Response response : responseList) {
      if (!response.getOtherError().isEmpty()) {
        return response.getOtherError();
      }
    }
    return null;
  }

  @Override
  @Nonnull
  public Iterator<Coprocessor.Response> iterator() {
    return responseList.iterator();
  }
}

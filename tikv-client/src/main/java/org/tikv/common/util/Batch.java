/*
 * Copyright 2020 PingCAP, Inc.
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

package org.tikv.common.util;

import com.google.protobuf.ByteString;
import org.tikv.common.region.TiRegion;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A Batch containing the region, a list of keys and/or values to send */
public class Batch {
  private final BackOffer backOffer;
  private final TiRegion region;
  private final List<ByteString> keys;
  private final List<ByteString> values;
  private final Map<ByteString, ByteString> map;

  public Batch(BackOffer backOffer, TiRegion region, List<ByteString> keys) {
    this.backOffer = ConcreteBackOffer.create(backOffer);
    this.region = region;
    this.keys = keys;
    this.values = null;
    this.map = null;
  }

  public Batch(
      BackOffer backOffer, TiRegion region, List<ByteString> keys, List<ByteString> values) {
    this.backOffer = ConcreteBackOffer.create(backOffer);
    this.region = region;
    this.keys = keys;
    this.values = values;
    this.map = toMap(keys, values);
  }

  private Map<ByteString, ByteString> toMap(List<ByteString> keys, List<ByteString> values) {
    assert keys.size() == values.size();
    Map<ByteString, ByteString> kvMap = new HashMap<>();
    for (int i = 0; i < keys.size(); i++) {
      kvMap.put(keys.get(i), values.get(i));
    }
    return kvMap;
  }

  public BackOffer getBackOffer() {
    return backOffer;
  }

  public TiRegion getRegion() {
    return region;
  }

  public List<ByteString> getKeys() {
    return keys;
  }

  public List<ByteString> getValues() {
    return values;
  }

  public Map<ByteString, ByteString> getMap() {
    return map;
  }
}

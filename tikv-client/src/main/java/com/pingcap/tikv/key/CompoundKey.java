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

package com.pingcap.tikv.key;

import com.google.common.base.Joiner;
import com.pingcap.tikv.codec.CodecDataOutput;
import java.util.ArrayList;
import java.util.List;

public class CompoundKey extends Key {

  private final List<Key> keys;

  protected CompoundKey(List<Key> keys, byte[] value) {
    super(value);
    this.keys = keys;
  }

  public static CompoundKey concat(Key lKey, Key rKey) {
    Builder builder = newBuilder();
    builder.append(lKey).append(rKey);
    return builder.build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public List<Key> getKeys() {
    return keys;
  }

  @Override
  public String toString() {
    return String.format("[%s]", Joiner.on(",").useForNull("Null").join(keys));
  }

  public static class Builder {
    private final List<Key> keys = new ArrayList<>();

    public Builder append(Key key) {
      if (key instanceof CompoundKey) {
        CompoundKey compKey = (CompoundKey) key;
        for (Key child : compKey.getKeys()) {
          append(child);
        }
      } else {
        keys.add(key);
      }
      return this;
    }

    public CompoundKey build() {
      int totalLen = 0;
      for (Key key : keys) {
        totalLen += key.getBytes().length;
      }
      CodecDataOutput cdo = new CodecDataOutput(totalLen);
      for (Key key : keys) {
        cdo.write(key.getBytes());
      }
      return new CompoundKey(keys, cdo.toBytes());
    }
  }
}

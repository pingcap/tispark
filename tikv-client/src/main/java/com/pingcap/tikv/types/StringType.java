/*
 *
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
 *
 */

package com.pingcap.tikv.types;

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.meta.TiColumnInfo;
import java.nio.charset.StandardCharsets;

public class StringType extends BytesType {
  public static final StringType VARCHAR = new StringType(MySQLType.TypeVarchar);
  public static final StringType BINARY = new StringType(MySQLType.TypeString);
  public static final StringType CHAR = new StringType(MySQLType.TypeString);

  public static final MySQLType[] subTypes =
      new MySQLType[] {MySQLType.TypeVarchar, MySQLType.TypeString};

  protected StringType(MySQLType tp) {
    super(tp);
  }

  protected StringType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  /** {@inheritDoc} */
  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    return new String((byte[]) super.decodeNotNull(flag, cdi), StandardCharsets.UTF_8);
  }
}

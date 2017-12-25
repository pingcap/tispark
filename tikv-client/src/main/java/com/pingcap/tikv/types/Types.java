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

public class Types {
  // The following ints are mysql type.
  // TYPE_DECIMAL is not used in MySQL.
  // public static final int TYPE_DECIMAL = 0;
  public static final int TYPE_TINY = 1;
  public static final int TYPE_SHORT = 2;
  public static final int TYPE_LONG = 3;
  public static final int TYPE_FLOAT = 4;
  public static final int TYPE_DOUBLE = 5;
  public static final int TYPE_NULL = 6;
  public static final int TYPE_TIMESTAMP = 7;
  public static final int TYPE_LONG_LONG = 8;
  public static final int TYPE_INT24 = 9;
  public static final int TYPE_DATE = 10;
  // Original name was TypeTime. But TiDB replace Time with
  // Duration to avoid name conflict. We just adopt this.
  public static final int TYPE_DURATION = 11;
  public static final int TYPE_DATETIME = 12;
  public static final int TYPE_YEAR = 13;
  public static final int TYPE_NEW_DATE = 14;
  public static final int TYPE_VARCHAR = 15;
  public static final int TYPE_BIT = 16;

  public static final int TYPE_JSON = 0xf5;
  public static final int TYPE_NEW_DECIMAL = 0xf6;
  public static final int TYPE_ENUM = 0xf7;
  public static final int TYPE_SET = 0xf8;
  public static final int TYPE_TINY_BLOB = 0xf9;
  public static final int TYPE_MEDIUM_BLOB = 0xfa;
  public static final int TYPE_LONG_BLOB = 0xfb;
  public static final int TYPE_BLOB = 0xfc;
  public static final int TYPE_VAR_STRING = 0xfd;
  public static final int TYPE_STRING = 0xfe;
  public static final int TYPE_GEOMETRY = 0xff;

  // Flag Information for strict mysql type
  public static final int NotNullFlag = 1; /* Field can't be NULL */
  public static final int PriKeyFlag = 2; /* Field is part of a primary key */
  public static final int UniqueKeyFlag = 4; /* Field is part of a unique key */
  public static final int MultipleKeyFlag = 8; /* Field is part of a key */
  public static final int BlobFlag = 16; /* Field is a blob */
  public static final int UnsignedFlag = 32; /* Field is unsigned */
  public static final int ZerofillFlag = 64; /* Field is zerofill */
  public static final int BinaryFlag = 128; /* Field is binary   */

  public static final int EnumFlag = 256; /* Field is an enum */
  public static final int AutoIncrementFlag = 512; /* Field is an auto increment field */
  public static final int TimestampFlag = 1024; /* Field is a timestamp */
  public static final int SetFlag = 2048; /* Field is a set */
  public static final int NoDefaultValueFlag = 4096; /* Field doesn't have a default value */
  public static final int OnUpdateNowFlag = 8192; /* Field is set to NOW on UPDATE */
  public static final int NumFlag = 32768; /* Field is a num (for clients) */
  public static final int PartKeyFlag = 16384; /* Intern: Part of some keys */
  public static final int GroupFlag = 32768; /* Intern: Group field */
  public static final int BinCmpFlag = 131072; /* Intern: Used by sql_yacc */
}

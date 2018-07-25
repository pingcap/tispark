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

package com.pingcap.tikv.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TiDBInfo {
  private long id;
  private String name;
  private String charset;
  private String collate;
  private List<TiTableInfo> tables;
  private SchemaState schemaState;

  @JsonCreator
  public TiDBInfo(
      @JsonProperty("id") long id,
      @JsonProperty("db_name") CIStr name,
      @JsonProperty("charset") String charset,
      @JsonProperty("collate") String collate,
      @JsonProperty("-") List<TiTableInfo> tables,
      @JsonProperty("state") int schemaState) {
    this.id = id;
    this.name = name.getL();
    this.charset = charset;
    this.collate = collate;
    this.tables = tables;
    this.schemaState = SchemaState.fromValue(schemaState);
  }

  public long getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getCharset() {
    return charset;
  }

  public String getCollate() {
    return collate;
  }

  public List<TiTableInfo> getTables() {
    return tables;
  }

  SchemaState getSchemaState() {
    return schemaState;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof TiDBInfo)) {
      return false;
    }
    TiDBInfo otherDB = (TiDBInfo)other;
    return otherDB.getId() == getId() && otherDB.getName().equals(getName());
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = prime + Long.hashCode(getId());
    return result * prime + getName().hashCode();
  }
}

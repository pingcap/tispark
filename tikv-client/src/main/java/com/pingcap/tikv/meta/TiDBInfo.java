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
import java.io.Serializable;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TiDBInfo implements Serializable {
  private final long id;
  private final String name;
  private final String charset;
  private final String collate;
  private final List<TiTableInfo> tables;
  private final SchemaState schemaState;

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

  private TiDBInfo(
      long id,
      String name,
      String charset,
      String collate,
      List<TiTableInfo> tables,
      SchemaState schemaState) {
    this.id = id;
    this.name = name;
    this.charset = charset;
    this.collate = collate;
    this.tables = tables;
    this.schemaState = schemaState;
  }

  public TiDBInfo rename(String newName) {
    return new TiDBInfo(id, newName, charset, collate, tables, schemaState);
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
    TiDBInfo otherDB = (TiDBInfo) other;
    return otherDB.getId() == getId() && otherDB.getName().equals(getName());
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = prime + Long.hashCode(getId());
    return result * prime + getName().hashCode();
  }
}

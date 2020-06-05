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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

// TiUserIdentity represents username and hostname.
public class TiUserIdentity implements Serializable {
  private final String username;
  private final String hostname;
  private final boolean currentUser;
  private final String authUsername;
  private final String authHostname;

  @JsonCreator
  public TiUserIdentity(
      @JsonProperty("Username") String userName,
      @JsonProperty("Hostname") String hostName,
      @JsonProperty("CurrentUser") boolean currentUser,
      @JsonProperty("AuthUsername") String authUserName,
      @JsonProperty("AuthHostname") String authHostName) {
    this.authHostname = authHostName;
    this.authUsername = authUserName;
    this.hostname = hostName;
    this.username = userName;
    this.currentUser = currentUser;
  }
}

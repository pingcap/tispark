package com.pingcap.tikv.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

// TiUserIdentity represents username and hostname.
public class TiUserIdentity implements Serializable {
  private String username;
  private String hostname;
  private boolean currentUser;
  private String authUsername;
  private String authHostname;

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

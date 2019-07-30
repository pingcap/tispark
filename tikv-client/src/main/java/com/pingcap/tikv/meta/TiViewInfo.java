package com.pingcap.tikv.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public class TiViewInfo implements Serializable {
  // ViewAlgorithm is VIEW's SQL AlGORITHM characteristic.
  // See https://dev.mysql.com/doc/refman/5.7/en/view-algorithms.html
  private final long viewAlgorithm;
  private final TiUserIdentity userIdentity;
  // ViewSecurity is VIEW's SQL SECURITY characteristic.
  // See https://dev.mysql.com/doc/refman/5.7/en/create-view.html
  private final long viewSecurity;
  private final String viewSelect;
  // ViewCheckOption is VIEW's WITH CHECK OPTION clause part.
  // See https://dev.mysql.com/doc/refman/5.7/en/view-check-option.html
  private final long viewCheckOpt;
  private final List<String> viewCols;

  @JsonCreator
  public TiViewInfo(
      @JsonProperty("view_algorithm") long viewAlgorithm,
      @JsonProperty("view_definer") TiUserIdentity userIdentity,
      @JsonProperty("view_security") long viewSecurity,
      @JsonProperty("view_select") String viewSelect,
      @JsonProperty("view_checkoption") long viewCheckOpt,
      @JsonProperty("view_cols") List<CIStr> viewCols) {
    this.viewAlgorithm = viewAlgorithm;
    this.userIdentity = userIdentity;
    this.viewSecurity = viewSecurity;
    this.viewSelect = viewSelect;
    this.viewCheckOpt = viewCheckOpt;
    this.viewCols = viewCols.stream().map(CIStr::getO).collect(Collectors.toList());
  }
}

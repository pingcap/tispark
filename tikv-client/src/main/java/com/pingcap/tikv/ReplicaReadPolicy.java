/*
 * Copyright 2022 PingCAP, Inc.
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

package com.pingcap.tikv;

import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.replica.Region;
import org.tikv.common.replica.ReplicaSelector;
import org.tikv.common.replica.Store;
import org.tikv.common.replica.Store.Label;

public class ReplicaReadPolicy implements ReplicaSelector {

  static final Logger LOG = LoggerFactory.getLogger(ReplicaReadPolicy.class);

  public static final ReplicaReadPolicy DEFAULT = ReplicaReadPolicy.create("leader", "", "", "");

  private final Map<String, String> labels;
  private final Set<String> whitelist;
  private final Set<String> blacklist;
  private final List<Role> roles;

  private ReplicaReadPolicy(
      final Map<String, String> labels,
      final Set<String> whitelist,
      final Set<String> blacklist,
      final List<Role> roles) {
    this.labels = labels;
    this.whitelist = whitelist;
    this.blacklist = blacklist;
    this.roles = roles;
  }

  @Override
  public List<Store> select(Region region) {
    Store leader = region.getLeader();
    Store[] stores = region.getStores();
    List<Store> followers =
        Arrays.stream(stores)
            .filter(store -> store.isFollower() && accept(store))
            .collect(Collectors.toList());
    List<Store> learners =
        Arrays.stream(stores)
            .filter(store -> store.isLearner() && accept(store))
            .collect(Collectors.toList());
    Collections.shuffle(followers);
    Collections.shuffle(learners);
    List<Store> candidates = new ArrayList<>(stores.length);
    for (Role role : roles) {
      switch (role) {
        case LEADER:
          candidates.add(leader);
          continue;
        case FOLLOWER:
          candidates.addAll(followers);
          continue;
        case LEARNER:
          candidates.addAll(learners);
          continue;
        default:
          continue;
      }
    }
    if (candidates.size() == 0) {
      throw new IllegalStateException("Can not get enough candidates");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Current candidates are: {}", candidates);
    }
    return candidates;
  }

  private static Map<String, String> extractLabels(String label) {
    String[] labels = label.split(",");
    Map<String, String> map = new HashMap<>(labels.length);
    String key;
    String value;
    for (String pair : labels) {
      if (pair.isEmpty()) {
        continue;
      }
      String[] pairs = pair.trim().split("=");
      if (pairs.length != 2
          || (key = pairs[0].trim()).isEmpty()
          || (value = pairs[1].trim()).isEmpty()) {
        throw new IllegalArgumentException(
            "Invalid replica read labels: " + Arrays.toString(labels));
      }
      map.put(key, value);
    }
    return map;
  }

  private static Set<String> extractList(String list) {
    return Arrays.stream(list.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toSet());
  }

  public static ReplicaReadPolicy create(
      String role, String label, String whitelist, String blacklist) {
    Map<String, String> labels = extractLabels(label);
    Set<String> whitelists = extractList(whitelist);
    Set<String> blacklists = extractList(blacklist);
    List<Role> roles =
        Arrays.stream(role.split(",")).map(Role::fromString).collect(Collectors.toList());
    return new ReplicaReadPolicy(labels, whitelists, blacklists, roles);
  }

  private boolean inWhitelist(Store store) {
    if (whitelist.isEmpty()) {
      return false;
    }
    return whitelist.stream().anyMatch(a -> a.equals(store.getAddress()));
  }

  private boolean notInBlacklist(Store store) {
    if (blacklist.isEmpty()) {
      return true;
    }
    return blacklist.stream().noneMatch(a -> a.equals(store.getAddress()));
  }

  private boolean matchLabels(Store store) {
    if (labels.isEmpty()) {
      return true;
    }
    int matched = 0;
    for (Label label : store.getLabels()) {
      if (label.getValue().equals(labels.get(label.getKey()))) {
        matched++;
      }
    }
    return matched == labels.size();
  }

  protected boolean accept(Store store) {
    return (matchLabels(store) || inWhitelist(store)) && notInBlacklist(store);
  }

  enum Role {
    LEADER,
    FOLLOWER,
    LEARNER;

    public static Role fromString(String role) {
      for (Role value : values()) {
        if (value.name().equalsIgnoreCase(role)) {
          return value;
        }
      }
      throw new IllegalArgumentException("Available roles are: " + Arrays.toString(values()));
    }
  }
}

/*
 * Copyright 2019 PingCAP, Inc.
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

package com.pingcap.tikv;

import java.io.IOException;
import org.junit.After;
import org.junit.Before;

public abstract class PDMockServerTest {
  protected static final String LOCAL_ADDR = "127.0.0.1";
  static final long CLUSTER_ID = 1024;
  protected static TiSession session;
  protected PDMockServer pdServer;

  @Before
  public void setUp() throws IOException {
    setUp(LOCAL_ADDR);
  }

  void setUp(String addr) throws IOException {
    pdServer = new PDMockServer();
    pdServer.start(CLUSTER_ID);
    pdServer.addGetMemberResp(
        GrpcUtils.makeGetMembersResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeMember(1, "http://" + addr + ":" + pdServer.port),
            GrpcUtils.makeMember(2, "http://" + addr + ":" + (pdServer.port + 1)),
            GrpcUtils.makeMember(3, "http://" + addr + ":" + (pdServer.port + 2))));
    TiConfiguration conf = TiConfiguration.createDefault(addr + ":" + pdServer.port);
    session = TiSession.getInstance(conf);
  }

  @After
  public void tearDown() throws Exception {
    session.close();
    pdServer.stop();
  }
}

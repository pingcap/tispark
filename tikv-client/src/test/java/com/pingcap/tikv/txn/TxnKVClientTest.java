/*
 * Copyright 2021 PingCAP, Inc.
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

package com.pingcap.tikv.txn;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doThrow;

import com.google.protobuf.ByteString;
import org.tikv.common.exception.KeyException;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.TiRegion;
import org.tikv.txn.TxnKVClient;
import org.tikv.txn.type.ClientRPCResult;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.tikv.kvproto.Kvrpcpb;

public class TxnKVClientTest {

  @Mock private RegionStoreClient.RegionStoreClientBuilder clientBuilder;

  @Mock private RegionStoreClient client;

  @Mock private TiRegion tiRegion;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    // mock RegionStoreClient
    Mockito.when(clientBuilder.build(tiRegion)).thenReturn(client);
    // mock KeyException with WriteConflict
    Kvrpcpb.KeyError.Builder errBuilder = Kvrpcpb.KeyError.newBuilder();
    errBuilder.setConflict(Kvrpcpb.WriteConflict.newBuilder());
    KeyException keyException = new KeyException(errBuilder.build(), "");
    doThrow(keyException)
        .when(client)
        .prewrite(null, ByteString.copyFromUtf8("writeConflict"), null, 0, 0);
  }

  @Test
  public void testPrewriteWithConflict() {
    TxnKVClient txnKVClient = new TxnKVClient(null, clientBuilder, null);
    ClientRPCResult result =
        txnKVClient.prewrite(null, null, ByteString.copyFromUtf8("writeConflict"), 0, 0, tiRegion);
    assertFalse(result.isRetry());
  }
}

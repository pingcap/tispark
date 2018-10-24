package com.pingcap.tikv;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.kvproto.Coprocessor;
import com.pingcap.tikv.kvproto.Kvrpcpb;
import com.pingcap.tikv.kvproto.Metapb;
import com.pingcap.tikv.region.TiRegion;
import org.junit.After;
import org.junit.Before;

public class MockServerTest {
    KVMockServer server;
    PDMockServer pdServer;
    static final String LOCAL_ADDR = "127.0.0.1";
    static final long CLUSTER_ID = 1024;
    int port;
    TiSession session;
    TiRegion region;

    @Before
    public void setUp() throws Exception {
        pdServer = new PDMockServer();
        pdServer.start(CLUSTER_ID);
        pdServer.addGetMemberResp(
                GrpcUtils.makeGetMembersResponse(
                        pdServer.getClusterId(),
                        GrpcUtils.makeMember(1, "http://" + LOCAL_ADDR + ":" + pdServer.port),
                        GrpcUtils.makeMember(2, "http://" + LOCAL_ADDR + ":" + (pdServer.port + 1)),
                        GrpcUtils.makeMember(2, "http://" + LOCAL_ADDR + ":" + (pdServer.port + 2))));

        Metapb.Region r =
                Metapb.Region.newBuilder()
                        .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(2))
                        .setId(233)
                        .setStartKey(ByteString.EMPTY)
                        .setEndKey(ByteString.EMPTY)
                        .addPeers(Metapb.Peer.newBuilder().setId(11).setStoreId(13))
                        .build();

        region = new TiRegion(r, r.getPeers(0), Kvrpcpb.IsolationLevel.RC, Kvrpcpb.CommandPri.Low);
        server = new KVMockServer();
        port = server.start(region);
        // No PD needed in this test
        TiConfiguration conf = TiConfiguration.createDefault("127.0.0.1:" + pdServer.port);
        session = TiSession.create(conf);
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    @VisibleForTesting
    protected static Coprocessor.KeyRange createByteStringRange(ByteString sKey, ByteString eKey) {
        return Coprocessor.KeyRange.newBuilder().setStart(sKey).setEnd(eKey).build();
    }
}

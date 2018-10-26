package com.pingcap.tikv;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.kvproto.Kvrpcpb;
import com.pingcap.tikv.kvproto.Metapb;
import com.pingcap.tikv.operation.iterator.RawScanIterator;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class KVRawClient {
  private static final String RAW_PREFIX = "raw_";
  private static final String DEFAULT_PD_ADDRESS = "127.0.0.1:2379";
  private final TiSession session;
  private final RegionManager regionManager;

  public KVRawClient(String addresses) {
    session = TiSession.create(TiConfiguration.createDefault(addresses));
    regionManager = session.getRegionManager();
  }

  public KVRawClient() {
    this(DEFAULT_PD_ADDRESS);
  }

  /**
   * Put a raw key-value pair to TiKV
   *
   * @param key raw key
   * @param value raw value
   */
  public void rawPut(ByteString key, ByteString value) {
    Pair<TiRegion, Metapb.Store> pair = regionManager.getRegionStorePairByRawKey(key);
    RegionStoreClient client =
        RegionStoreClient.create(pair.first, pair.second, session);
    client.rawPut(defaultBackOff(), key, value);
  }

  /**
   * Get a raw key-value pair from TiKV if key exists
   *
   * @param key raw key
   * @return a ByteString value if key exists, ByteString.EMPTY if key does not exist
   */
  public ByteString rawGet(ByteString key) {
    Pair<TiRegion, Metapb.Store> pair = regionManager.getRegionStorePairByRawKey(key);
    RegionStoreClient client =
        RegionStoreClient.create(pair.first, pair.second, session);
    return client.rawGet(defaultBackOff(), key);
  }

  /**
   * Delete a raw key-value pair from TiKV if key exists
   *
   * @param key raw key to be deleted
   */
  public void rawDelete(ByteString key) {
    TiRegion region = regionManager.getRegionByRawKey(key);
    Kvrpcpb.Context context =
        Kvrpcpb.Context.newBuilder()
            .setRegionId(region.getId())
            .setRegionEpoch(region.getRegionEpoch())
            .setPeer(region.getLeader())
            .build();
    Pair<TiRegion, Metapb.Store> pair = regionManager.getRegionStorePairByRawKey(key);
    RegionStoreClient client =
        RegionStoreClient.create(pair.first, pair.second, session);
    client.rawDelete(defaultBackOff(), key, context);
  }

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @return list of key-value pairs in range
   */
  public List<Kvrpcpb.KvPair> rawScan(ByteString startKey, ByteString endKey) {
    Iterator<Kvrpcpb.KvPair> iterator = rawScanIterator(startKey, endKey);
    List<Kvrpcpb.KvPair> result = new ArrayList<>();
    iterator.forEachRemaining(result::add);
    return result;
  }

  /**
   * Put a raw key-value pair with RAW_PREFIX into TiKV
   *
   * @param key utf-8 key
   * @param value utf-8 value
   */
  public void rawPutUtf8(String key, String value) {
    rawPut(rawKey(key), rawValue(value));
  }

  /**
   * Get a raw key-value pair from TiKV using utf-8 key with RAW_PREFIX
   *
   * @param key utf-8 key
   * @return value of key-value pair with key (RAW_PREFIX + key)
   */
  public ByteString rawGetUtf8(String key) {
    return rawGet(rawKey(key));
  }

  /**
   * Delete a raw key-value pair from TiKV using utf-8 key with RAW_PREFIX
   *
   * @param key utf-8 key
   */
  public void rawDeleteUtf8(String key) {
    rawDelete(rawKey(key));
  }

  /**
   * Scan a list of key-value pair from TiKV in range [RAW_PREFIX + startKey, RAW_PREFIX + endKey)
   *
   * @param startKey
   * @param endKey
   * @return
   */
  public List<Kvrpcpb.KvPair> rawScanUtf8(String startKey, String endKey) {
    return rawScan(rawKey(startKey), rawKey(endKey));
  }

  private Iterator<Kvrpcpb.KvPair> rawScanIterator(ByteString startKey, ByteString endKey) {
    return new RawScanIterator(startKey, endKey, session);
  }

  private BackOffer defaultBackOff() {
    return ConcreteBackOffer.newCustomBackOff(1000);
  }

  static ByteString rawKey(String key) {
    return ByteString.copyFromUtf8(RAW_PREFIX + key);
  }

  static ByteString rawValue(String value) {
    return ByteString.copyFromUtf8(value);
  }
}

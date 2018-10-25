package com.pingcap.tikv;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.kvproto.Kvrpcpb;
import com.pingcap.tikv.util.FastByteComparisons;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

public class rawKVTest {
  private KVRawClient client;

  private void checkPut(ByteString key, ByteString value) {
    client.rawPut(key, value);
    assert client.rawGet(key).equals(value);
  }

  private void checkScan(ByteString startKey, ByteString endKey, List<Kvrpcpb.KvPair> ans) {
    List<Kvrpcpb.KvPair> result = client.rawScan(startKey, endKey);
    assert result.equals(ans);
  }

  private void checkDelete(ByteString key) {
    client.rawDelete(key);
    checkEmpty(key);
  }

  private void checkEmpty(ByteString key) {
    assert client.rawGet(key).isEmpty();
  }

  private void checkPutUtf8(String key, String value) {
    client.rawPutUtf8(key, value);
    assert client.rawGetUtf8(key).toStringUtf8().equals(value);
  }

  private void checkScanUtf8(String startKey, String endKey, List<Kvrpcpb.KvPair> ans) {
    List<Kvrpcpb.KvPair> result = client.rawScanUtf8(startKey, endKey);
    assert result.equals(ans);
  }

  private void checkDeleteUtf8(String key) {
    client.rawDeleteUtf8(key);
    checkEmptyUtf8(key);
  }

  private void checkEmptyUtf8(String key) {
    assert client.rawGetUtf8(key).isEmpty();
  }

  private ByteString rawKey(String key) {
    return KVRawClient.rawKey(key);
  }

  private ByteString rawValue(String key) {
    return KVRawClient.rawValue(key);
  }

  @Test
  public void simpleTest() {
    try {
      client = new KVRawClient();
    } catch (Exception e) {
      System.out.println("Cannot initialize raw client. Test skipped.");
    }
    ByteString key = rawKey("key");
    ByteString key1 = rawKey("key1");
    ByteString key2 = rawKey("key2");
    ByteString key3 = rawKey("key3");
    ByteString value1 = rawValue("value1");
    ByteString value2 = rawValue("value2");
    Kvrpcpb.KvPair kv1 = Kvrpcpb.KvPair.newBuilder().setKey(key1).setValue(value1).build();
    Kvrpcpb.KvPair kv2 = Kvrpcpb.KvPair.newBuilder().setKey(key2).setValue(value2).build();

    checkEmpty(key1);
    checkEmpty(key2);
    checkPut(key1, value1);
    checkPut(key2, value2);
    List<Kvrpcpb.KvPair> result = new ArrayList<>();
    result.add(kv1);
    result.add(kv2);
    checkScan(key, key3, result);
    checkScan(key1, key3, result);
    checkDelete(key1);
    checkDelete(key2);

    checkEmptyUtf8("key1");
    checkEmptyUtf8("key2");
    checkPutUtf8("key1", "value1");
    checkPutUtf8("key2", "value2");
    checkScanUtf8("key", "key3", result);
    checkScanUtf8("key1", "key3", result);
    checkDeleteUtf8("key1");
    checkDeleteUtf8("key2");
  }

  private class ByteStringComparator implements Comparator<ByteString> {
    @Override
    public int compare(ByteString startKey, ByteString endKey) {
      return FastByteComparisons.compareTo(startKey.toByteArray(), endKey.toByteArray());
    }
  }

  private String getRandomString() {
    return RandomStringUtils.randomAlphanumeric(7, 18);
  }

  private ByteString getRandomRawKey() {
    return rawKey(getRandomString());
  }

  private ByteString getRandomValue() {
    return ByteString.copyFrom(getRandomString().getBytes());
  }

  private void checkScan(ByteString startKey, ByteString endKey, TreeMap<ByteString, ByteString> data) {
    Comparator<ByteString> bcmp = new ByteStringComparator();
    if (bcmp.compare(startKey, endKey) > 0) {
      ByteString tmp = startKey;
      startKey = endKey;
      endKey = tmp;
    }
    checkScan(startKey, endKey, data.subMap(startKey, endKey).entrySet().stream().map(kvPair -> Kvrpcpb.KvPair.newBuilder().setKey(kvPair.getKey()).setValue(kvPair.getValue()).build()).collect(Collectors.toList()));
  }

  private List<Kvrpcpb.KvPair> rawKeys() {
    return client.rawScan(rawKey(""), Key.toRawKey(rawKey("")).next().toByteString());
  }

  @Test
  public void test() {
    List<ByteString> keys = new ArrayList<>();
    List<ByteString> values = new ArrayList<>();
    TreeMap<ByteString, ByteString> data = new TreeMap<>(new ByteStringComparator());

    System.out.println("Initializing test");
    rawKeys().forEach(kvPair -> checkDelete(kvPair.getKey()));

    for (int i = 0; i < 100; i++) {
      keys.add(getRandomRawKey());
      values.add(getRandomValue());
    }
    Random r = new Random(1234);
    System.out.println("rawPut testing");
    for (int i = 0; i < 100; i++) {
      ByteString key = keys.get(i), value = values.get(r.nextInt(100));
      if (!data.containsKey(key)) {
        data.put(key, value);
        checkPut(key, value);
      }
    }
    System.out.println("rawScan testing");
    for (int i = 0; i < 1000; i++) {
      ByteString startKey = keys.get(r.nextInt(100)), endKey = keys.get(r.nextInt(100));
      checkScan(startKey, endKey, data);
    }
    for (int i = 0; i < 100; i++) {
      ByteString startKey = getRandomRawKey(), endKey = getRandomRawKey();
      checkScan(startKey, endKey, data);
    }
    System.out.println("rawDelete testing");
    for (ByteString key : data.keySet()) {
      checkDelete(key);
    }
    System.out.println(rawKeys().isEmpty() ? "ok, test done" : "no, something is wrong");
  }

}
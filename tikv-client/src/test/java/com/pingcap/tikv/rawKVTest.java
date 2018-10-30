package com.pingcap.tikv;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.kvproto.Kvrpcpb;
import com.pingcap.tikv.util.FastByteComparisons;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

public class rawKVTest {
  private KVRawClient client;
  private List<ByteString> keys;
  private List<ByteString> values;
  private TreeMap<ByteString, ByteString> data;
  private boolean initialized = false;
  private static final int KEY_POOL_SIZE = 100000;
  private Random r = new Random(1234);


  private void checkPut(ByteString key, ByteString value) {
    client.rawPut(key, value);
    assert client.rawGet(key).equals(value);
  }

  private void checkScan(ByteString startKey, ByteString endKey, List<Kvrpcpb.KvPair> ans) {
    List<Kvrpcpb.KvPair> result = client.rawScan(startKey, endKey);
    assert result.equals(ans);
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

  @Before
  public void setClient() {
    try {
      client = new KVRawClient();
      keys = new ArrayList<>();
      values = new ArrayList<>();
      data = new TreeMap<>(new ByteStringComparator());
      for (int i = 0; i < KEY_POOL_SIZE; i++) {
        keys.add(getRandomRawKey());
        values.add(getRandomValue());
      }
      initialized = true;
    } catch (Exception e) {
      System.out.println("Cannot initialize raw client. Test skipped.");
    }
  }

  @Test
  public void simpleTest() {
    if (!initialized) return;
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
    List<Kvrpcpb.KvPair> result2 = new ArrayList<>();
    result.add(kv1);
    result.add(kv2);
    checkScan(key, key3, result);
    checkScan(key1, key3, result);
    result2.add(kv1);
    checkScan(key, key2, result2);
    checkDelete(key1);
    checkDelete(key2);

    checkEmptyUtf8("key1");
    checkEmptyUtf8("key2");
    checkPutUtf8("key1", "value1");
    checkPutUtf8("key2", "value2");
    checkScanUtf8("key", "key3", result);
    checkScanUtf8("key1", "key3", result);
    checkScanUtf8("key", "key2", result2);
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

  private List<Kvrpcpb.KvPair> rawKeys() {
    return client.rawScan(rawKey(""), Key.toRawKey(rawKey("")).next().toByteString());
  }

  @Test
  public void testCorrectness() {
    if (!initialized) return;
    test(100, 100, 1000, 100, false);
  }

  @Test
  public void testSpeed() {
    if (!initialized) return;
    test(10000, 10000, 100, 5000, true);
  }

  public void test(int putCases, int getCases, int scanCases, int deleteCases, boolean speedTest) {
    if (putCases > KEY_POOL_SIZE) {
      System.out.println("Number of distinct keys required exceeded pool size " + KEY_POOL_SIZE);
      return;
    }
    if (deleteCases > putCases) {
      System.out.println("Number of keys to delete is more than total number of keys");
      return;
    }

    System.out.println("Initializing test");
    rawKeys().forEach(kvPair -> checkDelete(kvPair.getKey()));

    rawPutTest(putCases, speedTest);
    rawGetTest(getCases, speedTest);
    rawScanTest(scanCases, speedTest);
    rawDeleteTest(deleteCases, speedTest);

    for (ByteString key : data.keySet()) {
      checkDelete(key);
    }
    System.out.println(rawKeys().isEmpty() ? "ok, test done" : "no, something is wrong");
  }

  public void rawPutTest(int putCases, boolean speedTest) {
    System.out.println("rawPut testing");
    if (speedTest) {
      long start, end, time = 0;

      for (int i = 0; i < putCases; ) {
        ByteString key = keys.get(i), value = values.get(r.nextInt(KEY_POOL_SIZE));
        if (!data.containsKey(key)) {
          data.put(key, value);
          start = System.currentTimeMillis();
          client.rawPut(key, value);
          end = System.currentTimeMillis();
          time += end - start;
          i++;
        }
      }
      System.out.println(putCases + " rawPut: " + time / 1000.0 + "s");
    } else {
      for (int i = 0; i < putCases; i++) {
        ByteString key = keys.get(i), value = values.get(r.nextInt(KEY_POOL_SIZE));
        if (!data.containsKey(key)) {
          data.put(key, value);
          checkPut(key, value);
        }
      }
    }
  }

  public void rawScanTest(int scanCases, boolean speedTest) {
    System.out.println("rawScan testing");
    if (speedTest) {
      long start, end, time = 0;
      for (int i = 0; i < scanCases; i++) {
        ByteString startKey = keys.get(r.nextInt(KEY_POOL_SIZE)), endKey = keys.get(r.nextInt(KEY_POOL_SIZE));
        start = System.currentTimeMillis();
        client.rawScan(startKey, endKey);
        end = System.currentTimeMillis();
        time += end - start;
      }
      System.out.println(scanCases + " rawScan: " + time / 1000.0 + "s");
    } else {
      for (int i = 0; i < scanCases; i++) {
        ByteString startKey = keys.get(r.nextInt(KEY_POOL_SIZE)), endKey = keys.get(r.nextInt(KEY_POOL_SIZE));
        checkScan(startKey, endKey, data);
      }
    }
  }

  public void rawGetTest(int getCases, boolean speedTest) {
    System.out.println("rawGet testing");
    int i = 0;
    if (speedTest) {
      long start, end, time = 0;
      for (ByteString key : data.keySet()) {
        start = System.currentTimeMillis();
        client.rawGet(key);
        end = System.currentTimeMillis();
        time += end - start;
        i++;
        if (i >= getCases) {
          break;
        }
      }
      System.out.println(getCases + " rawGet: " + time / 1000.0 + "s");
    }
  }

  public void rawDeleteTest(int deleteCases, boolean speedTest) {
    System.out.println("rawDelete testing");
    int i = 0;
    if (speedTest) {
      long start, end, time = 0;
      for (ByteString key : data.keySet()) {
        start = System.currentTimeMillis();
        client.rawDelete(key);
        end = System.currentTimeMillis();
        time += end - start;
        i++;
        if (i >= deleteCases) {
          break;
        }
      }
      System.out.println(deleteCases + " rawDelete: " + time / 1000.0 + "s");
    } else {
      for (ByteString key : data.keySet()) {
        checkDelete(key);
        i++;
        if (i >= deleteCases) {
          break;
        }
      }
    }
  }

}
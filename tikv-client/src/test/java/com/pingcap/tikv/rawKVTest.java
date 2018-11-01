package com.pingcap.tikv;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.kvproto.Kvrpcpb;
import com.pingcap.tikv.util.FastByteComparisons;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.pingcap.tikv.KVRawClient.*;

public class rawKVTest {
  private static final int KEY_POOL_SIZE = 100000;
  private static final int PUT_CASES = 10000;
  private static final int WORKER_CNT = 100;
  private static final ExecutorService executors = Executors.newFixedThreadPool(WORKER_CNT);
  private static final ByteString RAW_START_KEY = ByteString.copyFromUtf8(RAW_PREFIX);
  private static final ByteString RAW_END_KEY = Key.toRawKey(RAW_START_KEY).next().toByteString();
  private KVRawClient client;
  private List<ByteString> keys;
  private List<ByteString> randomKeys;
  private List<ByteString> values;
  private TreeMap<ByteString, ByteString> data;
  private boolean initialized = false;
  private Random r = new Random(1234);
  private ByteStringComparator bsc = new ByteStringComparator();


  private void checkPut(ByteString key, ByteString value) {
    client.put(key, value);
    assert client.get(key).equals(value);
  }

  private void checkScan(ByteString startKey, ByteString endKey, List<Kvrpcpb.KvPair> ans) {
    List<Kvrpcpb.KvPair> result = client.scan(startKey, endKey);
    assert result.equals(ans);
  }

  private void checkScan(ByteString startKey, ByteString endKey, TreeMap<ByteString, ByteString> data) {
    checkScan(
        startKey, endKey,
        data.subMap(startKey, endKey)
            .entrySet()
            .stream()
            .map(kvPair -> Kvrpcpb.KvPair.newBuilder().setKey(kvPair.getKey()).setValue(kvPair.getValue()).build())
            .collect(Collectors.toList())
    );
  }

  private void checkDelete(ByteString key) {
    client.delete(key);
    checkEmpty(key);
  }

  private void checkEmpty(ByteString key) {
    assert client.get(key).isEmpty();
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

  @Before
  public void setClient() {
    try {
      if (client == null) {
        client = new KVRawClient();
      }
      keys = new ArrayList<>();
      randomKeys = new ArrayList<>();
      values = new ArrayList<>();
      data = new TreeMap<>(bsc);
      for (int i = 0; i < KEY_POOL_SIZE; i++) {
        keys.add(rawKey(String.valueOf(i)));
        randomKeys.add(getRandomRawKey());
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
    return client.scan(RAW_START_KEY, RAW_END_KEY);
  }

  @Test
  public void testCorrectness() {
    if (!initialized) return;
    test(100, 100, 100, 100, false);
  }

  @Test
  public void testSpeed() {
    if (!initialized) return;
    test(PUT_CASES, 10000, 100, 5000, true);
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
    List<Kvrpcpb.KvPair> remainingKeys = rawKeys();
    System.out.println("deleting " + remainingKeys.size());
    remainingKeys.forEach(kvPair -> checkDelete(kvPair.getKey()));

    rawPutTest(putCases, speedTest);
    rawGetTest(getCases, speedTest);
    rawScanTest(scanCases, speedTest);
    rawDeleteTest(deleteCases, speedTest);

    data.keySet().forEach(this::checkDelete);
    System.out.println((remainingKeys = rawKeys()).isEmpty() ? "ok, test done" : "no, something is wrong " + remainingKeys.size());
  }

  public void rawPutTest(int putCases, boolean speedTest) {
    System.out.println("put testing");
    if (speedTest) {
      for (int i = 0; i < putCases; i++) {
        ByteString key = keys.get(i), value = values.get(i);
        data.put(key, value);
      }

      long start = System.currentTimeMillis();
      int base = putCases / WORKER_CNT;
      for (int cnt = 0; cnt < WORKER_CNT; cnt++) {
        int i = cnt;
        executors.submit(() -> {
          for (int j = 0; j < base; j++) {
            int num = i * base + j;
            ByteString key = keys.get(num), value = values.get(num);
            client.put(key, value);
          }
        });
      }
      executors.shutdown();
      try {
        if (!executors.awaitTermination(100, TimeUnit.SECONDS)) {
          executors.shutdownNow();
        }
      } catch (InterruptedException e) {
        executors.shutdownNow();
        System.out.println("Test Interrupted\n" + e.getMessage());
      }
      long end = System.currentTimeMillis();
      System.out.println(putCases + " put: " + (end - start) / 1000.0 + "s workers=" + WORKER_CNT + " put=" + rawKeys().size());
    } else {
      for (int i = 0; i < putCases; i++) {
        ByteString key = randomKeys.get(i), value = values.get(r.nextInt(KEY_POOL_SIZE));
        data.put(key, value);
        checkPut(key, value);
      }
    }
  }

  public void rawGetTest(int getCases, boolean speedTest) {
    System.out.println("get testing");
    int i = 0;
    if (speedTest) {
      long start = System.currentTimeMillis();
      for (ByteString key : data.keySet()) {
        client.get(key);
        i++;
        if (i >= getCases) {
          break;
        }
      }
      long end = System.currentTimeMillis();
      System.out.println(getCases + " get: " + (end - start) / 1000.0 + "s");
    } else {
      for (Map.Entry<ByteString, ByteString> pair : data.entrySet()) {
        assert client.get(pair.getKey()).equals(pair.getValue());
        i++;
        if (i >= getCases) {
          break;
        }
      }
    }
  }

  public void rawScanTest(int scanCases, boolean speedTest) {
    System.out.println("rawBatchScan testing");
    if (speedTest) {
      long start, end, time = 0;
      for (int i = 0; i < scanCases; i++) {
        ByteString startKey = keys.get(r.nextInt(KEY_POOL_SIZE)), endKey = keys.get(r.nextInt(KEY_POOL_SIZE));
        if (bsc.compare(startKey, endKey) > 0) {
          ByteString tmp = startKey;
          startKey = endKey;
          endKey = tmp;
        }
        start = System.currentTimeMillis();
        client.scan(startKey, endKey);
        end = System.currentTimeMillis();
        time += end - start;
      }
      System.out.println(scanCases + " rawBatchScan: " + time / 1000.0 + "s");
    } else {
      for (int i = 0; i < scanCases; i++) {
        ByteString startKey = randomKeys.get(r.nextInt(KEY_POOL_SIZE)), endKey = randomKeys.get(r.nextInt(KEY_POOL_SIZE));
        if (bsc.compare(startKey, endKey) > 0) {
          ByteString tmp = startKey;
          startKey = endKey;
          endKey = tmp;
        }
        checkScan(startKey, endKey, data);
      }
    }
  }

  public void rawDeleteTest(int deleteCases, boolean speedTest) {
    System.out.println("delete testing");
    int i = 0;
    if (speedTest) {
      long start, end, time = 0;
      for (ByteString key : data.keySet()) {
        start = System.currentTimeMillis();
        client.delete(key);
        end = System.currentTimeMillis();
        time += end - start;
        i++;
        if (i >= deleteCases) {
          break;
        }
      }
      System.out.println(deleteCases + " delete: " + time / 1000.0 + "s");
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

  private class ByteStringComparator implements Comparator<ByteString> {
    @Override
    public int compare(ByteString startKey, ByteString endKey) {
      return FastByteComparisons.compareTo(startKey.toByteArray(), endKey.toByteArray());
    }
  }

}
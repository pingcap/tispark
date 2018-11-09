package com.pingcap.tikv;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.kvproto.Kvrpcpb;
import com.pingcap.tikv.util.FastByteComparisons;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class RawKVClientTest {
  private static final String RAW_PREFIX = "raw_";
  private static final int KEY_POOL_SIZE = 1000000;
  private static final int TEST_CASES = 10000;
  private static final int WORKER_CNT = 100;
  private static final ByteString RAW_START_KEY = ByteString.copyFromUtf8(RAW_PREFIX);
  private static final ByteString RAW_END_KEY = Key.toRawKey(RAW_START_KEY).next().toByteString();
  private RawKVClient client;
  private static final List<ByteString> orderedKeys;
  private static final List<ByteString> randomKeys;
  private static final List<ByteString> values;
  private TreeMap<ByteString, ByteString> data;
  private boolean initialized;
  private Random r = new Random(1234);
  private static final ByteStringComparator bsc = new ByteStringComparator();
  private static final ExecutorService executors = Executors.newFixedThreadPool(WORKER_CNT);
  private final ExecutorCompletionService<Object> completionService = new ExecutorCompletionService<>(executors);

  static {
    orderedKeys = new ArrayList<>();
    randomKeys = new ArrayList<>();
    values = new ArrayList<>();
    for (int i = 0; i < KEY_POOL_SIZE; i++) {
      orderedKeys.add(rawKey(String.valueOf(i)));
      randomKeys.add(getRandomRawKey());
      values.add(getRandomValue());
    }
  }

  private static String getRandomString() {
    return RandomStringUtils.randomAlphanumeric(7, 18);
  }

  private static ByteString getRandomRawKey() {
    return rawKey(getRandomString());
  }

  private static ByteString getRandomValue() {
    return ByteString.copyFrom(getRandomString().getBytes());
  }

  @Before
  public void setClient() {
    try {
      initialized = false;
      if (client == null) {
        client = RawKVClient.create();
      }
      data = new TreeMap<>(bsc);
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
  }

  private List<Kvrpcpb.KvPair> rawKeys() {
    return client.scan(RAW_START_KEY, RAW_END_KEY);
  }

  @Test
  public void validate() {
    if (!initialized) return;
    baseTest(100, 100, 100, 100, false);
  }

  /**
   * Example of benchmarking base test
   */
  @Test
  public void benchmark() {
    if (!initialized) return;
    baseTest(TEST_CASES, TEST_CASES, 200, 5000, true);
  }

  private void baseTest(int putCases, int getCases, int scanCases, int deleteCases, boolean benchmark) {
    if (putCases > KEY_POOL_SIZE) {
      System.out.println("Number of distinct orderedKeys required exceeded pool size " + KEY_POOL_SIZE);
      return;
    }
    if (deleteCases > putCases) {
      System.out.println("Number of orderedKeys to delete is more than total number of orderedKeys");
      return;
    }

    prepare();

    rawPutTest(putCases, benchmark);
    rawGetTest(getCases, benchmark);
    rawScanTest(scanCases, benchmark);
    rawDeleteTest(deleteCases, benchmark);

    prepare();
    System.out.println("ok, test done");
  }

  private void prepare() {
    System.out.println("Initializing test");
    List<Kvrpcpb.KvPair> remainingKeys = rawKeys();
    int sz = remainingKeys.size();
    System.out.println("deleting " + sz);
    int base = sz / WORKER_CNT;
    remainingKeys.forEach(kvPair -> checkDelete(kvPair.getKey()));
    for (int cnt = 0; cnt < WORKER_CNT; cnt++) {
      int i = cnt;
      completionService.submit(() -> {
        for (int j = 0; j < base; j++)
          checkDelete(remainingKeys.get(i * base + j).getKey());
        return null;
      });
    }
    awaitTimeOut(base / 100);
  }

  private void awaitTimeOut(int timeOutLimit) {
    try {
      for (int i = 0; i < WORKER_CNT; i++) {
        completionService.take().get(timeOutLimit, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.out.println("Current thread interrupted. Test fail.");
    } catch (TimeoutException e) {
      System.out.println("TimeOut Exceeded for current test. " + timeOutLimit + "s");
    } catch (ExecutionException e) {
      System.out.println("Execution exception met. Test fail.");
    }
  }

  private void rawPutTest(int putCases, boolean benchmark) {
    System.out.println("put testing");
    if (benchmark) {
      for (int i = 0; i < putCases; i++) {
        ByteString key = orderedKeys.get(i), value = values.get(i);
        data.put(key, value);
      }

      long start = System.currentTimeMillis();
      int base = putCases / WORKER_CNT;
      for (int cnt = 0; cnt < WORKER_CNT; cnt++) {
        int i = cnt;
        completionService.submit(() -> {
          for (int j = 0; j < base; j++) {
            int num = i * base + j;
            ByteString key = orderedKeys.get(num), value = values.get(num);
            client.put(key, value);
          }
          return null;
        });
      }
      awaitTimeOut(100);
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

  private void rawGetTest(int getCases, boolean benchmark) {
    System.out.println("get testing");
    if (benchmark) {
      long start = System.currentTimeMillis();
      int base = getCases / WORKER_CNT;
      for (int cnt = 0; cnt < WORKER_CNT; cnt++) {
        int i = cnt;
        completionService.submit(() -> {
          for (int j = 0; j < base; j++) {
            int num = i * base + j;
            ByteString key = orderedKeys.get(num);
            client.get(key);
          }
          return null;
        });
      }
      awaitTimeOut(200);
      long end = System.currentTimeMillis();
      System.out.println(getCases + " get: " + (end - start) / 1000.0 + "s");
    } else {
      int i = 0;
      for (Map.Entry<ByteString, ByteString> pair : data.entrySet()) {
        assert client.get(pair.getKey()).equals(pair.getValue());
        i++;
        if (i >= getCases) {
          break;
        }
      }
    }
  }

  private void rawScanTest(int scanCases, boolean benchmark) {
    System.out.println("rawScan testing");
    if (benchmark) {
      long start = System.currentTimeMillis();
      int base = scanCases / WORKER_CNT;
      for (int cnt = 0; cnt < WORKER_CNT; cnt++) {
        int i = cnt;
        completionService.submit(() -> {
          for (int j = 0; j < base; j++) {
            int num = i * base + j;
            ByteString startKey = randomKeys.get(num), endKey = randomKeys.get(num + 1);
            if (bsc.compare(startKey, endKey) > 0) {
              ByteString tmp = startKey;
              startKey = endKey;
              endKey = tmp;
            }
            client.scan(startKey, endKey);
          }
          return null;
        });
      }
      awaitTimeOut(200);
      long end = System.currentTimeMillis();
      System.out.println(scanCases + " scan: " + (end - start) / 1000.0 + "s");
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

  private void rawDeleteTest(int deleteCases, boolean benchmark) {
    System.out.println("delete testing");
    if (benchmark) {
      long start = System.currentTimeMillis();
      int base = deleteCases / WORKER_CNT;
      for (int cnt = 0; cnt < WORKER_CNT; cnt++) {
        int i = cnt;
        completionService.submit(() -> {
          for (int j = 0; j < base; j++) {
            int num = i * base + j;
            ByteString key = orderedKeys.get(num);
            client.delete(key);
          }
          return null;
        });
      }
      awaitTimeOut(100);
      long end = System.currentTimeMillis();
      System.out.println(deleteCases + " get: " + (end - start) / 1000.0 + "s");
    } else {
      int i = 0;
      for (ByteString key : data.keySet()) {
        checkDelete(key);
        i++;
        if (i >= deleteCases) {
          break;
        }
      }
    }
  }

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

  private static ByteString rawKey(String key) {
    return ByteString.copyFromUtf8(RAW_PREFIX + key);
  }

  private static ByteString rawValue(String value) {
    return ByteString.copyFromUtf8(value);
  }

  private static class ByteStringComparator implements Comparator<ByteString> {
    @Override
    public int compare(ByteString startKey, ByteString endKey) {
      return FastByteComparisons.compareTo(startKey.toByteArray(), endKey.toByteArray());
    }
  }

}

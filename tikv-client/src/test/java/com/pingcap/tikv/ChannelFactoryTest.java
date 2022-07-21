/*
 * Copyright 2022 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.pingcap.tikv;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import org.tikv.common.util.ChannelFactory;
import org.tikv.common.util.ChannelFactory.CertWatcher;
import org.tikv.shade.com.google.common.collect.ImmutableList;
import org.tikv.shade.io.grpc.ManagedChannel;

public class ChannelFactoryTest {
  private final AtomicLong ts = new AtomicLong(System.currentTimeMillis());
  private final String tlsPath = "../config/cert/pem/";
  private final String caPath = tlsPath + "root.pem";
  private final String clientCertPath = tlsPath + "client.pem";
  private final String clientKeyPath = tlsPath + "client-pkcs8.key";

  private ChannelFactory createFactory() {
    int v = 1024;
    return new ChannelFactory(v, 5, 10, caPath, clientCertPath, clientKeyPath);
  }

  private void touchCert() {
    ts.addAndGet(100_000_000);
    assertTrue(new File(caPath).setLastModified(ts.get()));
  }

  @Test
  public void testCertWatcher() throws InterruptedException {
    AtomicBoolean changed = new AtomicBoolean(false);
    File a = new File(caPath);
    File b = new File(clientCertPath);
    File c = new File(clientKeyPath);
    try (CertWatcher certWatcher =
        new CertWatcher(2, ImmutableList.of(a, b, c), () -> changed.set(true))) {
      Thread.sleep(5000);
      assertTrue(changed.get());
    }
  }

  @Test
  public void testCertWatcherWithExceptionTask() throws InterruptedException {
    AtomicInteger timesOfReloadTask = new AtomicInteger(0);
    CertWatcher certWatcher =
        new CertWatcher(
            1,
            ImmutableList.of(new File(caPath), new File(clientCertPath), new File(clientKeyPath)),
            () -> {
              timesOfReloadTask.getAndIncrement();
              touchCert();
              throw new RuntimeException("Mock exception in reload task");
            });

    Thread.sleep(5000);
    certWatcher.close();
    assertTrue(timesOfReloadTask.get() > 1);
  }

  @Test
  public void testMultiThreadTlsReload() throws InterruptedException {
    ChannelFactory factory = createFactory();

    int taskCount = Runtime.getRuntime().availableProcessors() * 2;
    List<Thread> tasks = new ArrayList<>(taskCount);
    for (int i = 0; i < taskCount; i++) {
      Thread t =
          new Thread(
              () -> {
                for (int j = 0; j < 100; j++) {
                  String addr = "127.0.0.1:237" + (j % 2 == 0 ? 9 : 8);
                  ManagedChannel c = factory.getChannel(addr);
                  assertNotNull(c);
                  c.shutdownNow();
                  try {
                    Thread.sleep(100);
                  } catch (InterruptedException ignore) {
                  }
                }
              });
      t.start();
      tasks.add(t);
    }
    Thread reactor =
        new Thread(
            () -> {
              for (int i = 0; i < 100; i++) {
                touchCert();
                try {
                  Thread.sleep(100);
                } catch (InterruptedException ignore) {
                }
              }
            });
    reactor.start();

    for (Thread t : tasks) {
      t.join();
    }
    reactor.join();

    factory.close();
    assertTrue(factory.connPool.isEmpty());
  }
}

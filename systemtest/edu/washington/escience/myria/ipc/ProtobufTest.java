package edu.washington.escience.myria.ipc;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.SocketInfo;
import edu.washington.escience.myria.parallel.TransportMessageSerializer;
import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myria.parallel.ipc.IPCMessage;
import edu.washington.escience.myria.parallel.ipc.IPCMessage.Data;
import edu.washington.escience.myria.parallel.ipc.SimpleBagInputBuffer;
import edu.washington.escience.myria.parallel.ipc.StreamIOChannelID;
import edu.washington.escience.myria.parallel.ipc.StreamOutputChannel;
import edu.washington.escience.myria.proto.ControlProto.ControlMessage;
import edu.washington.escience.myria.proto.TransportProto.TransportMessage;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.IPCUtils;
import edu.washington.escience.myria.util.TestUtils;
import edu.washington.escience.myria.util.Tuple;

public class ProtobufTest {
  @Rule
  public TestRule watcher =
      new TestWatcher() {
        @Override
        protected void starting(Description description) {
          LOGGER.warn("*********************************************");
          LOGGER.warn(String.format("Starting test: %s()...", description.getMethodName()));
          LOGGER.warn("*********************************************");
        };
      };

  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(ProtobufTest.class);

  // @Test
  public void protobufExhaustNoWaitTest() throws Exception {
    for (int i = 0; i < 1000; i++) {
      if (Math.random() > 0.5) {
        protobufMultiThreadNoWaitTest();
      } else {
        protobufSingleThreadNoWaitTest();
      }
      LOGGER.info("finished : #{} round.", i);
    }
  }

  // @Test
  public void protobufExhaustSendMessageTest() throws Exception {
    for (int i = 0; i < 1000; i++) {
      if (Math.random() > 0.5) {
        protobufSingleThreadSendMessageTest();
      } else {
        protobufMultiThreadSendMessageTest();
      }
      LOGGER.info("finished : #{} round.", i);
    }
  }

  // @Test
  public void protobufExhaustSeparatePoolTest() throws Exception {
    for (int i = 0; i < 1000; i++) {
      if (Math.random() > 0.5) {
        protobufSingleThreadSeparatePoolTest();
      } else {
        protobufMultiThreadSeparatePoolTest();
      }
      LOGGER.info("finished : #{} round.", i);
    }
  }

  // @Test
  public void protobufExhaustTest() throws Exception {
    for (int i = 0; i < 1000; i++) {
      if (Math.random() > 0.5) {
        protobufSingleThreadTest();
      } else {
        protobufMultiThreadTest();
      }
      LOGGER.info("finished : #{} round.", i);
    }
  }

  @Test
  public void protobufMultiThreadNoWaitTest() throws Exception {
    final int serverID = 0;
    final Random r = new Random();

    final int totalRestrict = 1000000;
    final int numThreads = r.nextInt(50) + 1;
    LOGGER.info("Num threads: " + numThreads);

    final int numTuplesEach = totalRestrict / numThreads;

    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuplesEach, 20);
    final long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.putLong(0, ids[i]);
      tbb.putString(1, names[i]);
    }

    final HashMap<Tuple, Integer> expectedOne = TestUtils.tupleBatchToTupleBag(tbb);
    final List<HashMap<Tuple, Integer>> toMerge =
        new ArrayList<HashMap<Tuple, Integer>>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      toMerge.add(expectedOne);
    }

    final HashMap<Tuple, Integer> expected = TestUtils.mergeBags(toMerge);

    final HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(serverID, new SocketInfo("localhost", 19901));

    final IPCConnectionPool connectionPool =
        IPCTestUtil.startIPCConnectionPool(
            serverID, computingUnits, null, new TransportMessageSerializer(), 10, 8, 2);

    ImmutableSet.Builder<StreamIOChannelID> ioSetBuilder = ImmutableSet.builder();
    for (int i = 0; i < numThreads; i++) {
      StreamIOChannelID ioChannelID =
          new StreamIOChannelID(ExchangePairID.newID().getLong(), serverID);
      ioSetBuilder.add(ioChannelID);
    }
    ImmutableSet<StreamIOChannelID> ioChannelSet = ioSetBuilder.build();
    final SimpleBagInputBuffer<TupleBatch> inputBuffer =
        new SimpleBagInputBuffer<TupleBatch>(connectionPool, ioChannelSet);
    inputBuffer.setAttachment(schema);
    inputBuffer.start(this);

    final List<TupleBatch> tbs = tbb.getAll();

    final Thread[] threads = new Thread[numThreads];
    final AtomicInteger numSent = new AtomicInteger();
    int i = 0;
    for (final StreamIOChannelID outChannelID : ioChannelSet) {
      final Thread tt =
          new Thread() {
            @Override
            public void run() {
              final StreamOutputChannel<TupleBatch> ch =
                  Preconditions.checkNotNull(
                      connectionPool.<TupleBatch>reserveLongTermConnection(
                          outChannelID.getRemoteID(), outChannelID.getStreamID()));
              try {
                for (final TupleBatch tm : tbs) {
                  ch.write(tm);
                  numSent.incrementAndGet();
                }
              } finally {
                ch.release();
              }
            }
          };
      threads[i++] = tt;
      tt.setName("protobufMultiThreadNoWaitTest-" + i);
      tt.setDaemon(false);
      tt.start();
    }
    for (final Thread t : threads) {
      t.join();
    }

    LOGGER.info("Total sent: " + numSent.get() + " TupleBatches");
    int numReceived = 0;
    final TupleBatchBuffer actualTBB = new TupleBatchBuffer(tbb.getSchema());
    IPCMessage.StreamData<TupleBatch> m = null;
    final int timeoutInSeconds = 10;
    try {
      while (!inputBuffer.isEOS() || !inputBuffer.isEmpty()) {
        m = inputBuffer.poll(timeoutInSeconds, TimeUnit.SECONDS);
        if (m == null) {
          throw new Exception("Timeout in retrieving data from receive buffer.");
        }
        final TupleBatch tm = m.getPayload();
        if (tm != null) {
          numReceived++;
          tm.compactInto(actualTBB);
        }
      }
    } finally {
      connectionPool.deRegisterStreamInput(inputBuffer);
      connectionPool.shutdown().await();
      connectionPool.releaseExternalResources();
    }
    final HashMap<Tuple, Integer> actual = TestUtils.tupleBatchToTupleBag(actualTBB);
    TestUtils.assertTupleBagEqual(expected, actual);
    LOGGER.info("Received: " + numReceived + " TupleBatches");
  }

  @Test
  public void protobufMultiThreadSendMessageTest() throws Exception {
    final int serverID = 0;
    final Random r = new Random();

    final int totalRestrict = 1000000;
    final int numThreads = r.nextInt(50) + 1;
    LOGGER.info("Num threads: " + numThreads);

    final int numMessagesEach = totalRestrict / numThreads;
    final HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(serverID, new SocketInfo("localhost", 19901));
    LinkedBlockingQueue<Data<TransportMessage>> shortMessageQueue =
        new LinkedBlockingQueue<IPCMessage.Data<TransportMessage>>();

    final IPCConnectionPool connectionPool =
        IPCTestUtil.startIPCConnectionPool(
            serverID,
            computingUnits,
            shortMessageQueue,
            new TransportMessageSerializer(),
            10,
            8,
            2);

    final Thread[] threads = new Thread[numThreads];
    final AtomicInteger numSent = new AtomicInteger();
    for (int i = 0; i < numThreads; i++) {
      final Thread tt =
          new Thread() {
            @Override
            public void run() {
              try {
                for (int i = 0; i < numMessagesEach; i++) {
                  connectionPool.sendShortMessage(serverID, IPCUtils.CONTROL_SHUTDOWN);
                  numSent.incrementAndGet();
                }
              } finally {
              }
            }
          };
      threads[i] = tt;
      tt.setName("protobufMultiThreadSendMessageTest-" + i);
      tt.setDaemon(false);
      tt.start();
    }
    for (final Thread t : threads) {
      t.join();
    }

    LOGGER.info("Total sent: " + numSent.get() + " messages");

    int numReceived = 0;
    IPCMessage.Data<TransportMessage> tmw = null;
    final int timeoutInSeconds = 10;
    while (numSent.get() > numReceived) {
      tmw = (shortMessageQueue.poll(timeoutInSeconds, TimeUnit.SECONDS));
      if (tmw == null) {
        throw new Exception("Timeout in retrieving data from receive buffer.");
      }
      TransportMessage tm = tmw.getPayload();
      if (tm.getType() == TransportMessage.Type.CONTROL
          && tm.getControlMessage().getType() == ControlMessage.Type.SHUTDOWN) {
        numReceived++;
      }
    }
    connectionPool.shutdown().await();
    connectionPool.releaseExternalResources();
    LOGGER.info("Received: " + numReceived + " messages");
    assertEquals(numSent.get(), numReceived);
  }

  @Test
  public void protobufMultiThreadSeparatePoolTest() throws Exception {

    final int serverID = 0;
    final int clientID = 1;
    final Random r = new Random();

    final int totalRestrict = 1000000;
    final int numThreads = r.nextInt(50) + 1;
    LOGGER.info("Num threads: " + numThreads);

    final int numTuplesEach = totalRestrict / numThreads;

    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuplesEach, 20);
    final long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.putLong(0, ids[i]);
      tbb.putString(1, names[i]);
    }

    final HashMap<Tuple, Integer> expectedOne = TestUtils.tupleBatchToTupleBag(tbb);
    final List<HashMap<Tuple, Integer>> toMerge =
        new ArrayList<HashMap<Tuple, Integer>>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      toMerge.add(expectedOne);
    }

    final HashMap<Tuple, Integer> expected = TestUtils.mergeBags(toMerge);

    final HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(serverID, new SocketInfo("localhost", 19901));
    computingUnits.put(1, new SocketInfo("localhost", 19902));

    final IPCConnectionPool serverConnectionPool =
        IPCTestUtil.startIPCConnectionPool(
            serverID, computingUnits, null, new TransportMessageSerializer(), 10, 8, 2);
    final IPCConnectionPool clientConnectionPool =
        IPCTestUtil.startIPCConnectionPool(
            1, computingUnits, null, new TransportMessageSerializer(), 10, 8, 2);

    ImmutableSet.Builder<StreamIOChannelID> inputSetBuilder = ImmutableSet.builder();
    ImmutableSet.Builder<StreamIOChannelID> outputSetBuilder = ImmutableSet.builder();
    for (int i = 0; i < numThreads; i++) {
      StreamIOChannelID outputChannelID =
          new StreamIOChannelID(ExchangePairID.newID().getLong(), serverID);
      inputSetBuilder.add(new StreamIOChannelID(outputChannelID.getStreamID(), clientID));
      outputSetBuilder.add(outputChannelID);
    }

    ImmutableSet<StreamIOChannelID> outputChannelSet = outputSetBuilder.build();

    final SimpleBagInputBuffer<TupleBatch> inputBuffer =
        new SimpleBagInputBuffer<TupleBatch>(serverConnectionPool, inputSetBuilder.build());
    inputBuffer.setAttachment(schema);
    inputBuffer.start(this);

    final List<TupleBatch> tbs = tbb.getAll();

    final ArrayList<Thread> threads = new ArrayList<Thread>(numThreads);
    final AtomicInteger numSent = new AtomicInteger();
    final ConcurrentLinkedQueue<ChannelFuture> cf = new ConcurrentLinkedQueue<ChannelFuture>();
    int idx = 0;
    for (final StreamIOChannelID outputChannelID : outputChannelSet) {
      final Thread tt =
          new Thread() {
            @Override
            public void run() {
              final StreamOutputChannel<TupleBatch> ch =
                  Preconditions.checkNotNull(
                      clientConnectionPool.<TupleBatch>reserveLongTermConnection(
                          outputChannelID.getRemoteID(), outputChannelID.getStreamID()));

              try {
                for (final TupleBatch tm : tbs) {
                  ch.write(tm);

                  numSent.incrementAndGet();
                }
              } finally {
                ch.release();
              }
            }
          };
      threads.add(tt);
      tt.setName("protobufMultiThreadSeparatePoolTest#" + idx++);
      tt.setDaemon(false);
      tt.start();
    }
    for (final Thread t : threads) {
      t.join();
    }
    for (final ChannelFuture cff : cf) {
      if (cff != null) {
        cff.awaitUninterruptibly();
      }
    }

    LOGGER.info("Total sent: " + numSent.get() + " TupleBatches");

    int numReceived = 0;
    final TupleBatchBuffer actualTBB = new TupleBatchBuffer(tbb.getSchema());
    IPCMessage.StreamData<TupleBatch> m = null;
    final int timeoutInSeconds = 10;
    try {
      while (!inputBuffer.isEOS() || !inputBuffer.isEmpty()) {
        m = inputBuffer.poll(timeoutInSeconds, TimeUnit.SECONDS);
        if (m == null) {
          throw new Exception("Timeout in retrieving data from receive buffer.");
        }
        final TupleBatch tm = m.getPayload();
        if (tm != null) {
          numReceived++;
          tm.compactInto(actualTBB);
        }
      }
    } finally {
      serverConnectionPool.deRegisterStreamInput(inputBuffer);
      final ChannelGroupFuture cgfClient = clientConnectionPool.shutdown();
      final ChannelGroupFuture cgfServer = serverConnectionPool.shutdown();
      cgfClient.await();
      cgfServer.await();
      clientConnectionPool.releaseExternalResources();
      serverConnectionPool.releaseExternalResources();
    }
    final HashMap<Tuple, Integer> actual = TestUtils.tupleBatchToTupleBag(actualTBB);
    TestUtils.assertTupleBagEqual(expected, actual);
    LOGGER.info("Received: " + numReceived + " TupleBatches");
  }

  @Test
  public void protobufMultiThreadTest() throws Exception {

    final int serverID = 0;
    final Random r = new Random();

    final int totalRestrict = 1000000;
    final int numThreads = r.nextInt(50) + 1;
    LOGGER.info("Num threads: " + numThreads);

    final int numTuplesEach = totalRestrict / numThreads;

    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuplesEach, 20);
    final long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.putLong(0, ids[i]);
      tbb.putString(1, names[i]);
    }

    final HashMap<Tuple, Integer> expectedOne = TestUtils.tupleBatchToTupleBag(tbb);
    final List<HashMap<Tuple, Integer>> toMerge =
        new ArrayList<HashMap<Tuple, Integer>>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      toMerge.add(expectedOne);
    }

    final HashMap<Tuple, Integer> expected = TestUtils.mergeBags(toMerge);

    final HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(serverID, new SocketInfo("localhost", 19901));

    final IPCConnectionPool connectionPool =
        IPCTestUtil.startIPCConnectionPool(
            serverID, computingUnits, null, new TransportMessageSerializer(), 10, 8, 2);

    ImmutableSet.Builder<StreamIOChannelID> ioSetBuilder = ImmutableSet.builder();
    for (int i = 0; i < numThreads; i++) {
      StreamIOChannelID outputChannelID =
          new StreamIOChannelID(ExchangePairID.newID().getLong(), serverID);
      ioSetBuilder.add(outputChannelID);
    }
    ImmutableSet<StreamIOChannelID> ioChannelSet = ioSetBuilder.build();
    final SimpleBagInputBuffer<TupleBatch> inputBuffer =
        new SimpleBagInputBuffer<TupleBatch>(connectionPool, ioChannelSet);
    inputBuffer.setAttachment(schema);
    inputBuffer.start(this);

    final List<TupleBatch> tbs = tbb.getAll();

    final Thread[] threads = new Thread[numThreads];
    final AtomicInteger numSent = new AtomicInteger();
    final ConcurrentLinkedQueue<ChannelFuture> cf = new ConcurrentLinkedQueue<ChannelFuture>();
    int idx = 0;
    for (final StreamIOChannelID cID : ioChannelSet) {
      final Thread tt =
          new Thread() {
            @Override
            public void run() {
              final StreamOutputChannel<TupleBatch> ch =
                  Preconditions.checkNotNull(
                      connectionPool.<TupleBatch>reserveLongTermConnection(
                          cID.getRemoteID(), cID.getStreamID()));

              try {
                for (final TupleBatch tm : tbs) {
                  ch.write(tm);
                  numSent.incrementAndGet();
                }
              } finally {
                ch.release();
              }
            }
          };
      threads[idx] = tt;
      tt.setName("protobufMultiThreadTest#" + idx++);
      tt.setDaemon(false);
      tt.start();
    }
    for (final Thread t : threads) {
      t.join();
    }
    for (final ChannelFuture cff : cf) {
      if (cff != null) {
        cff.awaitUninterruptibly();
      }
    }

    LOGGER.info("Total sent: " + numSent.get() + " TupleBatches");

    int numReceived = 0;
    final TupleBatchBuffer actualTBB = new TupleBatchBuffer(tbb.getSchema());
    IPCMessage.StreamData<TupleBatch> m = null;
    final int timeoutInSeconds = 10;
    try {
      while (!inputBuffer.isEOS() || !inputBuffer.isEmpty()) {
        m = inputBuffer.poll(timeoutInSeconds, TimeUnit.SECONDS);
        if (m == null) {
          throw new Exception("Timeout in retrieving data from receive buffer.");
        }
        final TupleBatch tm = m.getPayload();
        if (tm != null) {
          numReceived++;
          tm.compactInto(actualTBB);
        }
      }
    } finally {
      connectionPool.deRegisterStreamInput(inputBuffer);
      connectionPool.shutdown().await();
      connectionPool.releaseExternalResources();
    }
    assertEquals(numSent.get(), numReceived);
    final HashMap<Tuple, Integer> actual = TestUtils.tupleBatchToTupleBag(actualTBB);
    TestUtils.assertTupleBagEqual(expected, actual);
    LOGGER.info("Received: " + numReceived + " TupleBatches");
  }

  @Test
  public void protobufSingleThreadNoWaitTest() throws Exception {

    final int serverID = 0;
    final Random r = new Random();

    final int numTuplesEach = 1000000 / (1 + r.nextInt(5));

    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuplesEach, 20);
    final long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.putLong(0, ids[i]);
      tbb.putString(1, names[i]);
    }

    final HashMap<Tuple, Integer> expected = TestUtils.tupleBatchToTupleBag(tbb);

    final HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(serverID, new SocketInfo("localhost", 19901));

    final IPCConnectionPool connectionPool =
        IPCTestUtil.startIPCConnectionPool(
            serverID, computingUnits, null, new TransportMessageSerializer(), 10, 8, 2);
    StreamIOChannelID ioChannel = new StreamIOChannelID(ExchangePairID.newID().getLong(), serverID);
    final SimpleBagInputBuffer<TupleBatch> inputBuffer =
        new SimpleBagInputBuffer<TupleBatch>(connectionPool, ImmutableSet.of(ioChannel));
    inputBuffer.setAttachment(schema);
    inputBuffer.start(this);

    final List<TupleBatch> tbs = tbb.getAll();

    final AtomicInteger numSent = new AtomicInteger();
    final StreamOutputChannel<TupleBatch> ch =
        Preconditions.checkNotNull(
            connectionPool.<TupleBatch>reserveLongTermConnection(
                ioChannel.getRemoteID(), ioChannel.getStreamID()));

    try {
      for (final TupleBatch tm : tbs) {
        ch.write(tm);
        numSent.incrementAndGet();
      }
    } finally {
      ch.release();
    }

    LOGGER.info("Total sent: " + numSent.get() + " TupleBatches");

    int numReceived = 0;
    final TupleBatchBuffer actualTBB = new TupleBatchBuffer(tbb.getSchema());
    IPCMessage.StreamData<TupleBatch> m = null;
    final int timeoutInSeconds = 10;
    try {
      while (!inputBuffer.isEOS() || !inputBuffer.isEmpty()) {
        m = inputBuffer.poll(timeoutInSeconds, TimeUnit.SECONDS);
        if (m == null) {
          throw new Exception("Timeout in retrieving data from receive buffer.");
        }
        final TupleBatch tm = m.getPayload();
        if (tm != null) {
          numReceived++;
          tm.compactInto(actualTBB);
        }
      }
    } finally {
      connectionPool.deRegisterStreamInput(inputBuffer);
      connectionPool.shutdown().await();
      connectionPool.releaseExternalResources();
    }
    final HashMap<Tuple, Integer> actual = TestUtils.tupleBatchToTupleBag(actualTBB);
    TestUtils.assertTupleBagEqual(expected, actual);
    LOGGER.info("Received: " + numReceived + " TupleBatches");
  }

  @Test
  public void protobufSingleThreadSendMessageTest() throws Exception {
    final int serverID = 0;
    final Random r = new Random();

    final int numTuplesEach = 1000000 / (1 + r.nextInt(5));

    final HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(serverID, new SocketInfo("localhost", 19901));

    LinkedBlockingQueue<Data<TransportMessage>> shortMessageQueue =
        new LinkedBlockingQueue<IPCMessage.Data<TransportMessage>>();
    final IPCConnectionPool connectionPool =
        IPCTestUtil.startIPCConnectionPool(
            serverID,
            computingUnits,
            shortMessageQueue,
            new TransportMessageSerializer(),
            10,
            8,
            2);

    final AtomicInteger numSent = new AtomicInteger();
    try {
      for (int i = 0; i < numTuplesEach; i++) {
        connectionPool.sendShortMessage(serverID, IPCUtils.CONTROL_WORKER_HEARTBEAT);
        numSent.incrementAndGet();
      }
    } finally {
    }

    LOGGER.info("Total sent: " + numSent.get() + " messages");

    int numReceived = 0;

    IPCMessage.Data<TransportMessage> tmw = null;
    final int timeoutInSeconds = 10;
    while (numSent.get() > numReceived) {
      tmw = shortMessageQueue.poll(timeoutInSeconds, TimeUnit.SECONDS);
      if (tmw == null) {
        throw new Exception("Timeout in retrieving data from receive buffer.");
      }
      TransportMessage tm = tmw.getPayload();
      if (tm.getType() == TransportMessage.Type.CONTROL
          && tm.getControlMessage().getType() == ControlMessage.Type.WORKER_HEARTBEAT) {
        numReceived++;
      }
    }
    connectionPool.shutdown().await();
    connectionPool.releaseExternalResources();

    LOGGER.info("Received: " + numReceived + " messages");
    assertEquals(numSent.get(), numReceived);
  }

  @Test
  public void protobufSingleThreadSeparatePoolTest() throws Exception {

    final Random r = new Random();

    final int numTuplesEach = 1000000 / (1 + r.nextInt(5));

    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuplesEach, 20);
    final long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.putLong(0, ids[i]);
      tbb.putString(1, names[i]);
    }

    final HashMap<Tuple, Integer> expected = TestUtils.tupleBatchToTupleBag(tbb);

    final int serverID = 0;
    final int clientID = 1;

    final HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(serverID, new SocketInfo("localhost", 19901));
    computingUnits.put(clientID, new SocketInfo("localhost", 19902));

    final IPCConnectionPool connectionPoolServer =
        IPCTestUtil.startIPCConnectionPool(
            serverID, computingUnits, null, new TransportMessageSerializer(), 10, 8, 2);
    final IPCConnectionPool connectionPoolClient =
        IPCTestUtil.startIPCConnectionPool(
            clientID, computingUnits, null, new TransportMessageSerializer(), 10, 8, 2);

    final List<TupleBatch> tbs = tbb.getAll();

    final AtomicInteger numSent = new AtomicInteger();

    StreamIOChannelID outputChannelID =
        new StreamIOChannelID(ExchangePairID.newID().getLong(), serverID);

    ImmutableSet.Builder<StreamIOChannelID> inputSetBuilder = ImmutableSet.builder();
    inputSetBuilder.add(new StreamIOChannelID(outputChannelID.getStreamID(), clientID));

    final SimpleBagInputBuffer<TupleBatch> inputBuffer =
        new SimpleBagInputBuffer<TupleBatch>(connectionPoolServer, inputSetBuilder.build());
    inputBuffer.setAttachment(schema);
    inputBuffer.start(this);

    final StreamOutputChannel<TupleBatch> ch =
        Preconditions.checkNotNull(
            connectionPoolClient.<TupleBatch>reserveLongTermConnection(
                outputChannelID.getRemoteID(), outputChannelID.getStreamID()));

    try {
      for (final TupleBatch tm : tbs) {
        ch.write(tm);
        numSent.incrementAndGet();
      }
    } finally {
      ch.release();
    }

    LOGGER.info("Total sent: " + numSent.get() + " TupleBatches");

    int numReceived = 0;
    final TupleBatchBuffer actualTBB = new TupleBatchBuffer(tbb.getSchema());
    IPCMessage.Data<TupleBatch> m = null;
    final int timeoutInSeconds = 10;
    try {
      while (!inputBuffer.isEOS() || !inputBuffer.isEmpty()) {
        m = inputBuffer.poll(timeoutInSeconds, TimeUnit.SECONDS);
        if (m == null) {
          throw new Exception("Timeout in retrieving data from receive buffer.");
        }
        final TupleBatch tb = m.getPayload();
        if (tb != null) {
          tb.compactInto(actualTBB);
          numReceived++;
        }
      }
    } finally {
      connectionPoolServer.deRegisterStreamInput(inputBuffer);
      final ChannelGroupFuture cgfClient = connectionPoolClient.shutdown();
      final ChannelGroupFuture cgfServer = connectionPoolServer.shutdown();
      cgfClient.await();
      cgfServer.await();
      connectionPoolClient.releaseExternalResources();
      connectionPoolServer.releaseExternalResources();
    }
    final HashMap<Tuple, Integer> actual = TestUtils.tupleBatchToTupleBag(actualTBB);
    LOGGER.info("Received: " + numReceived + " TupleBatches");
    TestUtils.assertTupleBagEqual(expected, actual);
  }

  @Test(expected = NullPointerException.class)
  public void protobufSingleThreadSeparatePoolUnknownIPCIDTest() throws Exception {
    // The server IPC pool doesn't know the client IPC pool.

    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final int serverID = 0;
    final int clientID = 1;

    final HashMap<Integer, SocketInfo> computingUnitsClient = new HashMap<Integer, SocketInfo>();
    computingUnitsClient.put(serverID, new SocketInfo("localhost", 19901));
    computingUnitsClient.put(clientID, new SocketInfo("localhost", 19902));

    final HashMap<Integer, SocketInfo> computingUnitsServer = new HashMap<Integer, SocketInfo>();
    computingUnitsServer.put(serverID, new SocketInfo("localhost", 19901));

    final IPCConnectionPool connectionPoolServer =
        IPCTestUtil.startIPCConnectionPool(
            serverID, computingUnitsServer, null, new TransportMessageSerializer(), 10, 8, 2);
    final IPCConnectionPool connectionPoolClient =
        IPCTestUtil.startIPCConnectionPool(
            clientID, computingUnitsClient, null, new TransportMessageSerializer(), 10, 8, 2);

    StreamIOChannelID outputChannelID =
        new StreamIOChannelID(ExchangePairID.newID().getLong(), serverID);

    ImmutableSet.Builder<StreamIOChannelID> inputSetBuilder = ImmutableSet.builder();
    inputSetBuilder.add(new StreamIOChannelID(outputChannelID.getStreamID(), clientID));

    final SimpleBagInputBuffer<TupleBatch> inputBuffer =
        new SimpleBagInputBuffer<TupleBatch>(connectionPoolServer, inputSetBuilder.build());
    inputBuffer.setAttachment(schema);
    inputBuffer.start(this);

    StreamOutputChannel<TupleBatch> ch = null;
    try {
      ch =
          Preconditions.checkNotNull(
              connectionPoolClient.<TupleBatch>reserveLongTermConnection(
                  outputChannelID.getRemoteID(), outputChannelID.getStreamID()));

    } finally {
      if (ch != null) {
        ch.release();
      }
      connectionPoolServer.deRegisterStreamInput(inputBuffer);
      final ChannelGroupFuture cgfClient = connectionPoolClient.shutdown();
      final ChannelGroupFuture cgfServer = connectionPoolServer.shutdown();
      cgfClient.await();
      cgfServer.await();
      connectionPoolClient.releaseExternalResources();
      connectionPoolServer.releaseExternalResources();
    }
  }

  @Test
  public void protobufSingleThreadTest() throws Exception {
    final int serverID = 0;
    final Random r = new Random();

    final int numTuplesEach = 1000000 / (1 + r.nextInt(5));

    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuplesEach, 20);
    final long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.putLong(0, ids[i]);
      tbb.putString(1, names[i]);
    }

    final HashMap<Tuple, Integer> expected = TestUtils.tupleBatchToTupleBag(tbb);

    final HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(serverID, new SocketInfo("localhost", 19901));

    final IPCConnectionPool connectionPool =
        IPCTestUtil.startIPCConnectionPool(
            serverID, computingUnits, null, new TransportMessageSerializer(), 10, 8, 2);
    ImmutableSet.Builder<StreamIOChannelID> builder = ImmutableSet.builder();
    StreamIOChannelID channelID = new StreamIOChannelID(ExchangePairID.newID().getLong(), serverID);
    builder.add(channelID);

    final SimpleBagInputBuffer<TupleBatch> inputBuffer =
        new SimpleBagInputBuffer<TupleBatch>(connectionPool, builder.build());
    inputBuffer.setAttachment(schema);
    inputBuffer.start(this);

    final List<TupleBatch> tbs = tbb.getAll();

    final AtomicInteger numSent = new AtomicInteger();
    final StreamOutputChannel<TupleBatch> ch =
        Preconditions.checkNotNull(
            connectionPool.<TupleBatch>reserveLongTermConnection(
                serverID, channelID.getStreamID()));

    try {
      for (final TupleBatch tm : tbs) {
        ch.write(tm);
        numSent.incrementAndGet();
      }
    } finally {
      ch.release();
    }

    LOGGER.debug("Total sent: " + numSent.get() + " TupleBatches");

    int numReceived = 0;
    final TupleBatchBuffer actualTBB = new TupleBatchBuffer(tbb.getSchema());
    IPCMessage.StreamData<TupleBatch> m = null;
    final int timeoutInSeconds = 10;
    try {
      while (!inputBuffer.isEOS() || !inputBuffer.isEmpty()) {
        m = inputBuffer.poll(timeoutInSeconds, TimeUnit.SECONDS);
        if (m == null) {
          throw new Exception("Timeout in retrieving data from receive buffer.");
        }
        final TupleBatch tm = m.getPayload();
        if (tm != null) {
          tm.compactInto(actualTBB);
        }
      }
    } finally {
      connectionPool.deRegisterStreamInput(inputBuffer);
      connectionPool.shutdown().await();
      connectionPool.releaseExternalResources();
    }
    final HashMap<Tuple, Integer> actual = TestUtils.tupleBatchToTupleBag(actualTBB);
    TestUtils.assertTupleBagEqual(expected, actual);
    LOGGER.info("Received: " + numReceived + " TupleBatches");
  }
}

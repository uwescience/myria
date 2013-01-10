package edu.washington.escience.myriad.ipc;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.junit.Test;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.parallel.IPCConnectionPool;
import edu.washington.escience.myriad.parallel.SocketInfo;
import edu.washington.escience.myriad.parallel.Worker.MessageWrapper;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage.DataMessageType;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.systemtest.SystemTestBase.Tuple;
import edu.washington.escience.myriad.util.IPCUtils;
import edu.washington.escience.myriad.util.TestUtils;

public class ProtobufTest {
  /** The logger for this class. Defaults to myriad.ipc level. */
  // private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ProtobufTest.class.getPackage().toString());
  private static final Logger LOGGER = Logger.getLogger(ProtobufTest.class.getPackage().toString());

  @Test
  public void protobufSingleThreadSendMessageTest() throws IOException, InterruptedException {

    Random r = new Random();

    int numTuplesEach = 1000000 / (1 + r.nextInt(5));

    String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuplesEach, 20);
    long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema = new Schema(new Type[] { Type.LONG_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }

    HashMap<Tuple, Integer> expected = TestUtils.tupleBatchToTupleBag(tbb);

    // final InetSocketAddress serverAddress = new InetSocketAddress("localhost", 19901);

    final LinkedBlockingQueue<MessageWrapper> messageQueue = new LinkedBlockingQueue<MessageWrapper>();

    HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(0, new SocketInfo("localhost", 19901));

    final IPCConnectionPool connectionPool = new IPCConnectionPool(0, computingUnits, messageQueue);
    connectionPool.start();
    // ServerBootstrap acceptor = ParallelUtility.createMasterIPCServer(messageQueue, connectionPool);
    // Channel server = acceptor.bind(serverAddress);

    final ExchangePairID epID = ExchangePairID.fromExisting(0l);
    final List<TransportMessage> tbs = tbb.getAllAsTM(epID);

    final AtomicInteger numSent = new AtomicInteger();
    final ChannelFuture cf;
    try {
      for (TransportMessage tm : tbs) {
        connectionPool.sendShortMessage(0, tm);
        numSent.incrementAndGet();
      }
      connectionPool.sendShortMessage(0, IPCUtils.eosTM(epID));
      numSent.incrementAndGet();
    } finally {
    }

    LOGGER.info("Total sent: " + numSent.get() + " TupleBatches");
    System.out.println("Pre shutdown");
    connectionPool.shutdown().awaitUninterruptibly();
    System.out.println("Post shutdown");

    int numReceived = 0;
    TupleBatchBuffer actualTBB = new TupleBatchBuffer(tbb.getSchema());
    int numEOS = 0;
    MessageWrapper m = null;
    while ((m = messageQueue.poll()) != null) {
      TransportMessage tm = m.message;
      if (tm.getType() == TransportMessage.TransportMessageType.DATA) {
        numReceived++;
        final DataMessage data = tm.getData();
        switch (data.getType().getNumber()) {
          case DataMessageType.EOS_VALUE:
            numEOS += 1;
            break;
          case DataMessageType.NORMAL_VALUE:
            final List<ColumnMessage> columnMessages = data.getColumnsList();
            final Column<?>[] columnArray = new Column[columnMessages.size()];
            int idx = 0;
            for (final ColumnMessage cm : columnMessages) {
              columnArray[idx++] = ColumnFactory.columnFromColumnMessage(cm);
            }
            final List<Column<?>> columns = Arrays.asList(columnArray);
            TupleBatch tb = new TupleBatch(schema, columns, columnArray[0].size());

            tb.compactInto(actualTBB);
            break;
        }
      }
    }
    assertEquals(1, numEOS);
    org.junit.Assert.assertEquals(numSent.get(), numReceived);
    HashMap<Tuple, Integer> actual = TestUtils.tupleBatchToTupleBag(actualTBB);
    TestUtils.assertTupleBagEqual(expected, actual);
    LOGGER.info("Received: " + numReceived + " TupleBatches");

  }

  @Test
  public void protobufSingleThreadTest() throws IOException, InterruptedException {

    Random r = new Random();

    int numTuplesEach = 1000000 / (1 + r.nextInt(5));

    String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuplesEach, 20);
    long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema = new Schema(new Type[] { Type.LONG_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }

    HashMap<Tuple, Integer> expected = TestUtils.tupleBatchToTupleBag(tbb);

    // final InetSocketAddress serverAddress = new InetSocketAddress("localhost", 19901);

    final LinkedBlockingQueue<MessageWrapper> messageQueue = new LinkedBlockingQueue<MessageWrapper>();

    HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(0, new SocketInfo("localhost", 19901));

    final IPCConnectionPool connectionPool = new IPCConnectionPool(0, computingUnits, messageQueue);
    connectionPool.start();
    // ServerBootstrap acceptor = ParallelUtility.createMasterIPCServer(messageQueue, connectionPool);
    // Channel server = acceptor.bind(serverAddress);

    final ExchangePairID epID = ExchangePairID.fromExisting(0l);
    final List<TransportMessage> tbs = tbb.getAllAsTM(epID);

    final AtomicInteger numSent = new AtomicInteger();
    final ChannelFuture cf;
    Channel ch = connectionPool.reserveLongTermConnection(0);
    try {
      for (TransportMessage tm : tbs) {
        ch.write(tm);
        numSent.incrementAndGet();
      }
      cf = ch.write(IPCUtils.eosTM(epID));
      numSent.incrementAndGet();
    } finally {
      connectionPool.releaseLongTermConnection(ch);
    }
    if (cf != null) {
      cf.awaitUninterruptibly();
    } else {
      System.out.println("cf is null!");
    }

    LOGGER.info("Total sent: " + numSent.get() + " TupleBatches");
    System.out.println("Pre shutdown");
    connectionPool.shutdown().awaitUninterruptibly();
    System.out.println("Post shutdown");

    int numReceived = 0;
    TupleBatchBuffer actualTBB = new TupleBatchBuffer(tbb.getSchema());
    int numEOS = 0;
    MessageWrapper m = null;
    while ((m = messageQueue.poll()) != null) {
      TransportMessage tm = m.message;
      if (tm.getType() == TransportMessage.TransportMessageType.DATA) {
        numReceived++;
        final DataMessage data = tm.getData();
        switch (data.getType().getNumber()) {
          case DataMessageType.EOS_VALUE:
            numEOS += 1;
            break;
          case DataMessageType.NORMAL_VALUE:
            final List<ColumnMessage> columnMessages = data.getColumnsList();
            final Column<?>[] columnArray = new Column[columnMessages.size()];
            int idx = 0;
            for (final ColumnMessage cm : columnMessages) {
              columnArray[idx++] = ColumnFactory.columnFromColumnMessage(cm);
            }
            final List<Column<?>> columns = Arrays.asList(columnArray);
            TupleBatch tb = new TupleBatch(schema, columns, columnArray[0].size());

            tb.compactInto(actualTBB);
            break;
        }
      }
    }
    assertEquals(1, numEOS);
    org.junit.Assert.assertEquals(numSent.get(), numReceived);
    HashMap<Tuple, Integer> actual = TestUtils.tupleBatchToTupleBag(actualTBB);
    TestUtils.assertTupleBagEqual(expected, actual);
    LOGGER.info("Received: " + numReceived + " TupleBatches");

  }

  @Test
  public void protobufSingleThreadSeparatePoolTest() throws IOException, InterruptedException {

    Random r = new Random();

    int numTuplesEach = 1000000 / (1 + r.nextInt(5));

    String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuplesEach, 20);
    long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema = new Schema(new Type[] { Type.LONG_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }

    HashMap<Tuple, Integer> expected = TestUtils.tupleBatchToTupleBag(tbb);

    // final InetSocketAddress serverAddress = new InetSocketAddress("localhost", 19901);

    final LinkedBlockingQueue<MessageWrapper> serverMessageQueue = new LinkedBlockingQueue<MessageWrapper>();
    final LinkedBlockingQueue<MessageWrapper> clientMessageQueue = new LinkedBlockingQueue<MessageWrapper>();

    HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(0, new SocketInfo("localhost", 19901));
    computingUnits.put(1, new SocketInfo("localhost", 19902));

    final IPCConnectionPool connectionPoolClient = new IPCConnectionPool(1, computingUnits, clientMessageQueue);
    connectionPoolClient.start();
    final IPCConnectionPool connectionPoolServer = new IPCConnectionPool(0, computingUnits, serverMessageQueue);
    connectionPoolServer.start(); // ServerBootstrap acceptor = ParallelUtility.createMasterIPCServer(messageQueue,
                                  // connectionPool);
    // Channel server = acceptor.bind(serverAddress);

    final ExchangePairID epID = ExchangePairID.fromExisting(0l);
    final List<TransportMessage> tbs = tbb.getAllAsTM(epID);

    final AtomicInteger numSent = new AtomicInteger();
    final ChannelFuture cf;
    Channel ch = connectionPoolClient.reserveLongTermConnection(0);
    try {
      for (TransportMessage tm : tbs) {
        ch.write(tm);
        numSent.incrementAndGet();
      }
      cf = ch.write(IPCUtils.eosTM(epID));
      numSent.incrementAndGet();
    } finally {
      connectionPoolClient.releaseLongTermConnection(ch);
    }
    if (cf != null) {
      cf.awaitUninterruptibly();
    } else {
      System.out.println("cf is null!");
    }

    LOGGER.info("Total sent: " + numSent.get() + " TupleBatches");
    System.out.println("Pre shutdown");
    ChannelGroupFuture cgfClient = connectionPoolClient.shutdown();
    ChannelGroupFuture cgfServer = connectionPoolServer.shutdown();
    cgfClient.awaitUninterruptibly();
    cgfServer.awaitUninterruptibly();

    System.out.println("Post shutdown");

    int numReceived = 0;
    TupleBatchBuffer actualTBB = new TupleBatchBuffer(tbb.getSchema());
    int numEOS = 0;
    MessageWrapper m = null;
    while ((m = serverMessageQueue.poll()) != null) {
      TransportMessage tm = m.message;
      if (tm.getType() == TransportMessage.TransportMessageType.DATA) {
        numReceived++;
        final DataMessage data = tm.getData();
        switch (data.getType().getNumber()) {
          case DataMessageType.EOS_VALUE:
            numEOS += 1;
            break;
          case DataMessageType.NORMAL_VALUE:
            final List<ColumnMessage> columnMessages = data.getColumnsList();
            final Column<?>[] columnArray = new Column[columnMessages.size()];
            int idx = 0;
            for (final ColumnMessage cm : columnMessages) {
              columnArray[idx++] = ColumnFactory.columnFromColumnMessage(cm);
            }
            final List<Column<?>> columns = Arrays.asList(columnArray);
            TupleBatch tb = new TupleBatch(schema, columns, columnArray[0].size());

            tb.compactInto(actualTBB);
            break;
        }
      }
    }
    assertEquals(1, numEOS);
    org.junit.Assert.assertEquals(numSent.get(), numReceived);
    HashMap<Tuple, Integer> actual = TestUtils.tupleBatchToTupleBag(actualTBB);
    TestUtils.assertTupleBagEqual(expected, actual);
    LOGGER.info("Received: " + numReceived + " TupleBatches");

  }

  @Test
  public void protobufSingleThreadNoWaitTest() throws IOException, InterruptedException {

    Random r = new Random();

    int numTuplesEach = 1000000 / (1 + r.nextInt(5));

    String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuplesEach, 20);
    long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema = new Schema(new Type[] { Type.LONG_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }

    HashMap<Tuple, Integer> expected = TestUtils.tupleBatchToTupleBag(tbb);

    final LinkedBlockingQueue<MessageWrapper> messageQueue = new LinkedBlockingQueue<MessageWrapper>();

    HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(0, new SocketInfo("localhost", 19901));

    final IPCConnectionPool connectionPool = new IPCConnectionPool(0, computingUnits, messageQueue);
    connectionPool.start();

    final ExchangePairID epID = ExchangePairID.fromExisting(0l);
    final List<TransportMessage> tbs = tbb.getAllAsTM(epID);

    final AtomicInteger numSent = new AtomicInteger();
    Channel ch = connectionPool.reserveLongTermConnection(0);
    try {
      for (TransportMessage tm : tbs) {
        ch.write(tm);
        numSent.incrementAndGet();
      }
      ch.write(IPCUtils.eosTM(epID));
      numSent.incrementAndGet();
    } finally {
      connectionPool.releaseLongTermConnection(ch);
    }

    LOGGER.info("Total sent: " + numSent.get() + " TupleBatches");
    System.out.println("Pre shutdown");
    connectionPool.shutdown().awaitUninterruptibly();
    System.out.println("Post shutdown");

    int numReceived = 0;
    TupleBatchBuffer actualTBB = new TupleBatchBuffer(tbb.getSchema());
    int numEOS = 0;
    MessageWrapper m = null;
    while ((m = messageQueue.poll()) != null) {
      TransportMessage tm = m.message;
      if (tm.getType() == TransportMessage.TransportMessageType.DATA) {
        numReceived++;
        final DataMessage data = tm.getData();
        switch (data.getType().getNumber()) {
          case DataMessageType.EOS_VALUE:
            numEOS += 1;
            break;
          case DataMessageType.NORMAL_VALUE:
            final List<ColumnMessage> columnMessages = data.getColumnsList();
            final Column<?>[] columnArray = new Column[columnMessages.size()];
            int idx = 0;
            for (final ColumnMessage cm : columnMessages) {
              columnArray[idx++] = ColumnFactory.columnFromColumnMessage(cm);
            }
            final List<Column<?>> columns = Arrays.asList(columnArray);
            TupleBatch tb = new TupleBatch(schema, columns, columnArray[0].size());

            tb.compactInto(actualTBB);
            break;
        }
      }
    }
    assertEquals(1, numEOS);
    org.junit.Assert.assertEquals(numSent.get(), numReceived);
    HashMap<Tuple, Integer> actual = TestUtils.tupleBatchToTupleBag(actualTBB);
    TestUtils.assertTupleBagEqual(expected, actual);
    LOGGER.info("Received: " + numReceived + " TupleBatches");

  }

  // @Test
  public void protobufExhaustSendMessageTest() throws IOException, InterruptedException {
    for (int i = 0; i < 1000; i++) {
      if (Math.random() > 0.5) {
        protobufSingleThreadSendMessageTest();
      } else {
        protobufMultiThreadSendMessageTest();
      }
      System.out.println("\nFinish round #" + i + "\n");
    }
  }

  // @Test
  public void protobufExhaustNoWaitTest() throws IOException, InterruptedException {
    for (int i = 0; i < 1000; i++) {
      if (Math.random() > 0.5) {
        protobufMultiThreadNoWaitTest();
      } else {
        protobufSingleThreadNoWaitTest();
      }
      System.out.println("\nFinish round #" + i + "\n");
    }
  }

  // @Test
  public void protobufExhaustTest() throws IOException, InterruptedException {
    for (int i = 0; i < 1000; i++) {
      if (Math.random() > 0.5) {
        protobufSingleThreadTest();
      } else {
        protobufMultiThreadTest();
      }
      System.out.println("\nFinish round #" + i + "\n");
    }
  }

  // @Test
  public void protobufExhaustSeparatePoolTest() throws IOException, InterruptedException {
    for (int i = 0; i < 1000; i++) {
      if (Math.random() > 0.5) {
        protobufSingleThreadSeparatePoolTest();
      } else {
        protobufMultiThreadSeparatePoolTest();
      }
      System.out.println("\nFinish round #" + i + "\n");
    }
  }

  @Test
  public void protobufMultiThreadTest() throws IOException, InterruptedException {

    Random r = new Random();

    int totalRestrict = 1000000;
    int numThreads = r.nextInt(50) + 1;
    LOGGER.info("Num threads: " + numThreads);

    int numTuplesEach = totalRestrict / numThreads;

    String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuplesEach, 20);
    long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema = new Schema(new Type[] { Type.LONG_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }

    HashMap<Tuple, Integer> expectedOne = TestUtils.tupleBatchToTupleBag(tbb);
    List<HashMap<Tuple, Integer>> toMerge = new ArrayList<HashMap<Tuple, Integer>>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      toMerge.add(expectedOne);
    }

    HashMap<Tuple, Integer> expected = TestUtils.mergeBags(toMerge);

    // final InetSocketAddress serverAddress = new InetSocketAddress("localhost", 19901);

    final LinkedBlockingQueue<MessageWrapper> messageQueue = new LinkedBlockingQueue<MessageWrapper>();

    HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(0, new SocketInfo("localhost", 19901));

    final IPCConnectionPool connectionPool = new IPCConnectionPool(0, computingUnits, messageQueue);
    // ServerBootstrap acceptor = ParallelUtility.createMasterIPCServer(messageQueue, connectionPool);
    // Channel server = acceptor.bind(serverAddress);
    connectionPool.start();

    final ExchangePairID epID = ExchangePairID.fromExisting(0l);
    final List<TransportMessage> tbs = tbb.getAllAsTM(epID);

    Thread[] threads = new Thread[numThreads];
    final AtomicInteger numSent = new AtomicInteger();
    final ConcurrentLinkedQueue<ChannelFuture> cf = new ConcurrentLinkedQueue<ChannelFuture>();
    for (int i = 0; i < numThreads; i++) {
      final int j = i;
      Thread tt = new Thread() {
        @Override
        public void run() {
          Channel ch = connectionPool.reserveLongTermConnection(0);
          System.out.println("reserved a connection: " + ch + ", connected: " + ch.isConnected());
          try {
            for (TransportMessage tm : tbs) {
              ch.write(tm);
              numSent.incrementAndGet();
            }
            ChannelFuture eosFuture = ch.write(IPCUtils.eosTM(epID));
            cf.add(eosFuture);
            eosFuture.addListener(new ChannelFutureListener() {

              @Override
              public void operationComplete(ChannelFuture future) throws Exception {

              }
            });
            numSent.incrementAndGet();
          } finally {
            connectionPool.releaseLongTermConnection(ch);
          }
        }
      };
      threads[i] = tt;
      tt.setDaemon(false);
      tt.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    for (ChannelFuture cff : cf) {
      if (cff != null) {
        cff.awaitUninterruptibly();
      }
    }

    LOGGER.info("Total sent: " + numSent.get() + " TupleBatches");
    System.out.println("Pre shutdown");
    connectionPool.shutdown().awaitUninterruptibly();
    System.out.println("Post shutdown");
    int numReceived = 0;
    TupleBatchBuffer actualTBB = new TupleBatchBuffer(tbb.getSchema());
    int numEOS = 0;
    MessageWrapper m = null;
    while ((m = messageQueue.poll()) != null) {
      TransportMessage tm = m.message;
      if (tm.getType() == TransportMessage.TransportMessageType.DATA) {
        numReceived++;
        final DataMessage data = tm.getData();
        switch (data.getType().getNumber()) {
          case DataMessageType.EOS_VALUE:
            numEOS += 1;
            break;
          case DataMessageType.NORMAL_VALUE:
            final List<ColumnMessage> columnMessages = data.getColumnsList();
            final Column<?>[] columnArray = new Column[columnMessages.size()];
            int idx = 0;
            for (final ColumnMessage cm : columnMessages) {
              columnArray[idx++] = ColumnFactory.columnFromColumnMessage(cm);
            }
            final List<Column<?>> columns = Arrays.asList(columnArray);
            TupleBatch tb = new TupleBatch(schema, columns, columnArray[0].size());

            tb.compactInto(actualTBB);
            break;
        }
      }
    }
    assertEquals(numThreads, numEOS);
    org.junit.Assert.assertEquals(numSent.get(), numReceived);
    HashMap<Tuple, Integer> actual = TestUtils.tupleBatchToTupleBag(actualTBB);
    TestUtils.assertTupleBagEqual(expected, actual);
    LOGGER.info("Received: " + numReceived + " TupleBatches");

  }

  @Test
  public void protobufMultiThreadSeparatePoolTest() throws IOException, InterruptedException {

    Random r = new Random();

    int totalRestrict = 1000000;
    int numThreads = r.nextInt(50) + 1;
    LOGGER.info("Num threads: " + numThreads);

    int numTuplesEach = totalRestrict / numThreads;

    String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuplesEach, 20);
    long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema = new Schema(new Type[] { Type.LONG_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }

    HashMap<Tuple, Integer> expectedOne = TestUtils.tupleBatchToTupleBag(tbb);
    List<HashMap<Tuple, Integer>> toMerge = new ArrayList<HashMap<Tuple, Integer>>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      toMerge.add(expectedOne);
    }

    HashMap<Tuple, Integer> expected = TestUtils.mergeBags(toMerge);

    // final InetSocketAddress serverAddress = new InetSocketAddress("localhost", 19901);

    final LinkedBlockingQueue<MessageWrapper> serverMessageQueue = new LinkedBlockingQueue<MessageWrapper>();
    final LinkedBlockingQueue<MessageWrapper> clientMessageQueue = new LinkedBlockingQueue<MessageWrapper>();

    HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(0, new SocketInfo("localhost", 19901));
    computingUnits.put(1, new SocketInfo("localhost", 19902));

    final IPCConnectionPool serverConnectionPool = new IPCConnectionPool(0, computingUnits, serverMessageQueue);
    final IPCConnectionPool clientConnectionPool = new IPCConnectionPool(1, computingUnits, clientMessageQueue);
    // ServerBootstrap acceptor = ParallelUtility.createMasterIPCServer(messageQueue, connectionPool);
    // Channel server = acceptor.bind(serverAddress);

    clientConnectionPool.start();
    serverConnectionPool.start();

    final ExchangePairID epID = ExchangePairID.fromExisting(0l);
    final List<TransportMessage> tbs = tbb.getAllAsTM(epID);

    Thread[] threads = new Thread[numThreads];
    final AtomicInteger numSent = new AtomicInteger();
    final ConcurrentLinkedQueue<ChannelFuture> cf = new ConcurrentLinkedQueue<ChannelFuture>();
    for (int i = 0; i < numThreads; i++) {
      final int j = i;
      Thread tt = new Thread() {
        @Override
        public void run() {
          Channel ch = clientConnectionPool.reserveLongTermConnection(0);
          System.out.println("reserved a connection: " + ch + ", connected: " + ch.isConnected());
          try {
            for (TransportMessage tm : tbs) {
              ch.write(tm);
              numSent.incrementAndGet();
            }
            ChannelFuture eosFuture = ch.write(IPCUtils.eosTM(epID));
            cf.add(eosFuture);
            eosFuture.addListener(new ChannelFutureListener() {

              @Override
              public void operationComplete(ChannelFuture future) throws Exception {

              }
            });
            numSent.incrementAndGet();
          } finally {
            clientConnectionPool.releaseLongTermConnection(ch);
          }
        }
      };
      threads[i] = tt;
      tt.setDaemon(false);
      tt.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    for (ChannelFuture cff : cf) {
      if (cff != null) {
        cff.awaitUninterruptibly();
      }
    }

    LOGGER.info("Total sent: " + numSent.get() + " TupleBatches");
    System.out.println("Pre shutdown");
    ChannelGroupFuture cgfClient = clientConnectionPool.shutdown();
    ChannelGroupFuture cgfServer = serverConnectionPool.shutdown();
    cgfClient.awaitUninterruptibly();
    cgfServer.awaitUninterruptibly();
    System.out.println("Post shutdown");
    int numReceived = 0;
    TupleBatchBuffer actualTBB = new TupleBatchBuffer(tbb.getSchema());
    int numEOS = 0;
    MessageWrapper m = null;
    while ((m = serverMessageQueue.poll()) != null) {
      TransportMessage tm = m.message;
      if (tm.getType() == TransportMessage.TransportMessageType.DATA) {
        numReceived++;
        final DataMessage data = tm.getData();
        switch (data.getType().getNumber()) {
          case DataMessageType.EOS_VALUE:
            numEOS += 1;
            break;
          case DataMessageType.NORMAL_VALUE:
            final List<ColumnMessage> columnMessages = data.getColumnsList();
            final Column<?>[] columnArray = new Column[columnMessages.size()];
            int idx = 0;
            for (final ColumnMessage cm : columnMessages) {
              columnArray[idx++] = ColumnFactory.columnFromColumnMessage(cm);
            }
            final List<Column<?>> columns = Arrays.asList(columnArray);
            TupleBatch tb = new TupleBatch(schema, columns, columnArray[0].size());

            tb.compactInto(actualTBB);
            break;
        }
      }
    }
    assertEquals(numThreads, numEOS);
    org.junit.Assert.assertEquals(numSent.get(), numReceived);
    HashMap<Tuple, Integer> actual = TestUtils.tupleBatchToTupleBag(actualTBB);
    TestUtils.assertTupleBagEqual(expected, actual);
    LOGGER.info("Received: " + numReceived + " TupleBatches");

  }

  @Test
  public void protobufMultiThreadNoWaitTest() throws IOException, InterruptedException {

    Random r = new Random();

    int totalRestrict = 1000000;
    int numThreads = r.nextInt(50) + 1;
    LOGGER.info("Num threads: " + numThreads);

    int numTuplesEach = totalRestrict / numThreads;

    String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuplesEach, 20);
    long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema = new Schema(new Type[] { Type.LONG_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }

    HashMap<Tuple, Integer> expectedOne = TestUtils.tupleBatchToTupleBag(tbb);
    List<HashMap<Tuple, Integer>> toMerge = new ArrayList<HashMap<Tuple, Integer>>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      toMerge.add(expectedOne);
    }

    HashMap<Tuple, Integer> expected = TestUtils.mergeBags(toMerge);

    // final InetSocketAddress serverAddress = new InetSocketAddress("localhost", 19901);

    final LinkedBlockingQueue<MessageWrapper> messageQueue = new LinkedBlockingQueue<MessageWrapper>();

    HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(0, new SocketInfo("localhost", 19901));

    final IPCConnectionPool connectionPool = new IPCConnectionPool(0, computingUnits, messageQueue);
    // ServerBootstrap acceptor = ParallelUtility.createMasterIPCServer(messageQueue, connectionPool);
    // Channel server = acceptor.bind(serverAddress);
    connectionPool.start();

    final ExchangePairID epID = ExchangePairID.fromExisting(0l);
    final List<TransportMessage> tbs = tbb.getAllAsTM(epID);

    Thread[] threads = new Thread[numThreads];
    final AtomicInteger numSent = new AtomicInteger();
    for (int i = 0; i < numThreads; i++) {
      final int j = i;
      Thread tt = new Thread() {
        @Override
        public void run() {
          Channel ch = connectionPool.reserveLongTermConnection(0);
          System.out.println("Reserved a longterm connection: " + ch + ", connected: " + ch.isConnected());
          try {
            for (TransportMessage tm : tbs) {
              ch.write(tm);
              numSent.incrementAndGet();
            }
            ch.write(IPCUtils.eosTM(epID));
            numSent.incrementAndGet();
          } finally {
            connectionPool.releaseLongTermConnection(ch);
          }
        }
      };
      threads[i] = tt;
      tt.setDaemon(false);
      tt.start();
    }
    for (Thread t : threads) {
      t.join();
    }

    LOGGER.info("Total sent: " + numSent.get() + " TupleBatches");
    System.out.println("Pre shutdown");
    connectionPool.shutdown().awaitUninterruptibly();
    System.out.println("Post shutdown");
    int numReceived = 0;
    TupleBatchBuffer actualTBB = new TupleBatchBuffer(tbb.getSchema());
    int numEOS = 0;
    MessageWrapper m = null;
    while ((m = messageQueue.poll()) != null) {
      TransportMessage tm = m.message;
      if (tm.getType() == TransportMessage.TransportMessageType.DATA) {
        numReceived++;
        final DataMessage data = tm.getData();
        switch (data.getType().getNumber()) {
          case DataMessageType.EOS_VALUE:
            numEOS += 1;
            break;
          case DataMessageType.NORMAL_VALUE:
            final List<ColumnMessage> columnMessages = data.getColumnsList();
            final Column<?>[] columnArray = new Column[columnMessages.size()];
            int idx = 0;
            for (final ColumnMessage cm : columnMessages) {
              columnArray[idx++] = ColumnFactory.columnFromColumnMessage(cm);
            }
            final List<Column<?>> columns = Arrays.asList(columnArray);
            TupleBatch tb = new TupleBatch(schema, columns, columnArray[0].size());

            tb.compactInto(actualTBB);
            break;
        }
      }
    }
    assertEquals(numThreads, numEOS);
    org.junit.Assert.assertEquals(numSent.get(), numReceived);
    HashMap<Tuple, Integer> actual = TestUtils.tupleBatchToTupleBag(actualTBB);
    TestUtils.assertTupleBagEqual(expected, actual);
    LOGGER.info("Received: " + numReceived + " TupleBatches");

  }

  @Test
  public void protobufMultiThreadSendMessageTest() throws IOException, InterruptedException {

    Random r = new Random();

    int totalRestrict = 1000000;
    int numThreads = r.nextInt(50) + 1;
    LOGGER.info("Num threads: " + numThreads);

    int numTuplesEach = totalRestrict / numThreads;

    String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuplesEach, 20);
    long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema = new Schema(new Type[] { Type.LONG_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }

    HashMap<Tuple, Integer> expectedOne = TestUtils.tupleBatchToTupleBag(tbb);
    List<HashMap<Tuple, Integer>> toMerge = new ArrayList<HashMap<Tuple, Integer>>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      toMerge.add(expectedOne);
    }

    HashMap<Tuple, Integer> expected = TestUtils.mergeBags(toMerge);

    // final InetSocketAddress serverAddress = new InetSocketAddress("localhost", 19901);

    final LinkedBlockingQueue<MessageWrapper> messageQueue = new LinkedBlockingQueue<MessageWrapper>();

    HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(0, new SocketInfo("localhost", 19901));

    final IPCConnectionPool connectionPool = new IPCConnectionPool(0, computingUnits, messageQueue);
    // ServerBootstrap acceptor = ParallelUtility.createMasterIPCServer(messageQueue, connectionPool);
    // Channel server = acceptor.bind(serverAddress);
    connectionPool.start();

    final ExchangePairID epID = ExchangePairID.fromExisting(0l);
    final List<TransportMessage> tbs = tbb.getAllAsTM(epID);

    Thread[] threads = new Thread[numThreads];
    final AtomicInteger numSent = new AtomicInteger();
    for (int i = 0; i < numThreads; i++) {
      Thread tt = new Thread() {
        @Override
        public void run() {
          try {
            for (TransportMessage tm : tbs) {
              connectionPool.sendShortMessage(0, tm);
              numSent.incrementAndGet();
            }
            connectionPool.sendShortMessage(0, IPCUtils.eosTM(epID));
            numSent.incrementAndGet();
          } finally {
          }
        }
      };
      threads[i] = tt;
      tt.setDaemon(false);
      tt.start();
    }
    for (Thread t : threads) {
      t.join();
    }

    LOGGER.info("Total sent: " + numSent.get() + " TupleBatches");
    System.out.println("Pre shutdown");
    connectionPool.shutdown().awaitUninterruptibly();
    System.out.println("Post shutdown");
    int numReceived = 0;
    TupleBatchBuffer actualTBB = new TupleBatchBuffer(tbb.getSchema());
    int numEOS = 0;
    MessageWrapper m = null;
    while ((m = messageQueue.poll()) != null) {
      TransportMessage tm = m.message;
      if (tm.getType() == TransportMessage.TransportMessageType.DATA) {
        numReceived++;
        final DataMessage data = tm.getData();
        switch (data.getType().getNumber()) {
          case DataMessageType.EOS_VALUE:
            numEOS += 1;
            break;
          case DataMessageType.NORMAL_VALUE:
            final List<ColumnMessage> columnMessages = data.getColumnsList();
            final Column<?>[] columnArray = new Column[columnMessages.size()];
            int idx = 0;
            for (final ColumnMessage cm : columnMessages) {
              columnArray[idx++] = ColumnFactory.columnFromColumnMessage(cm);
            }
            final List<Column<?>> columns = Arrays.asList(columnArray);
            TupleBatch tb = new TupleBatch(schema, columns, columnArray[0].size());

            tb.compactInto(actualTBB);
            break;
        }
      }
    }
    assertEquals(numThreads, numEOS);
    org.junit.Assert.assertEquals(numSent.get(), numReceived);
    HashMap<Tuple, Integer> actual = TestUtils.tupleBatchToTupleBag(actualTBB);
    TestUtils.assertTupleBagEqual(expected, actual);
    LOGGER.info("Received: " + numReceived + " TupleBatches");

  }
}

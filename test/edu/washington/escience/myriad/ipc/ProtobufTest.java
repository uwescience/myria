package edu.washington.escience.myriad.ipc;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.parallel.IPCConnectionPool;
import edu.washington.escience.myriad.parallel.ParallelUtility;
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
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger("edu.washington.escience.myriad.ipc");

  @Test
  public void protobufMultiThreadTest() throws IOException, InterruptedException {

    Random r = new Random();

    int totalRestrict = 1000000;
    int numThreads = r.nextInt(50) + 1;
    LOGGER.debug("Num threads: " + numThreads);

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

    final InetSocketAddress serverAddress = new InetSocketAddress("localhost", 19901);

    final LinkedBlockingQueue<MessageWrapper> messageQueue = new LinkedBlockingQueue<MessageWrapper>();
    ServerBootstrap acceptor = ParallelUtility.createMasterIPCServer(messageQueue);
    Channel server = acceptor.bind(serverAddress);

    HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(0, new SocketInfo("localhost", 19901));

    final IPCConnectionPool connectionPool = new IPCConnectionPool(0, computingUnits, messageQueue);

    final ExchangePairID epID = ExchangePairID.fromExisting(0l);
    final List<TransportMessage> tbs = tbb.getAllAsTM(epID);

    Thread[] threads = new Thread[numThreads];
    final AtomicInteger numSent = new AtomicInteger();
    final ChannelFuture[] cf = new ChannelFuture[numThreads];
    for (int i = 0; i < numThreads; i++) {
      final int j = i;
      Thread tt = new Thread() {
        @Override
        public void run() {
          Channel ch = connectionPool.get(0, 3, null);
          for (TransportMessage tm : tbs) {
            ch.write(tm);
            numSent.incrementAndGet();
          }
          cf[j] = ch.write(IPCUtils.eosTM(epID));
          numSent.incrementAndGet();
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

    LOGGER.debug("Total sent: " + numSent.get() + " TupleBatches");
    connectionPool.shutdown();
    server.close();
    server.disconnect();
    server.unbind().awaitUninterruptibly();
    Iterator<MessageWrapper> it = messageQueue.iterator();
    int numReceived = 0;
    TupleBatchBuffer actualTBB = new TupleBatchBuffer(tbb.getSchema());
    int numEOS = 0;
    while (it.hasNext()) {
      MessageWrapper m = it.next();
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
    LOGGER.debug("Received: " + numReceived + " TupleBatches");

  }
}

package edu.washington.escience.myriad.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.junit.Test;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.parallel.IPCConnectionPool;
import edu.washington.escience.myriad.parallel.ParallelUtility;
import edu.washington.escience.myriad.parallel.SocketInfo;
import edu.washington.escience.myriad.parallel.Worker.MessageWrapper;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage.DataMessageType;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage.TransportMessageType;
import edu.washington.escience.myriad.systemtest.SystemTestBase;
import edu.washington.escience.myriad.table._TupleBatch;

public class ProtobufTest {

  @Test
  public void protobufMultiThreadTest() throws IOException, InterruptedException {

    Random r = new Random();

    int totalRestrict = 1000000;
    int numThreads = r.nextInt(50) + 1;
    System.out.println("Num threads: " + numThreads);

    int numTuplesEach = totalRestrict / numThreads;

    String[] names = SystemTestBase.randomFixedLengthNumericString(1000, 1005, numTuplesEach, 20);
    long[] ids = SystemTestBase.randomLong(1000, 1005, names.length);

    final Schema schema = new Schema(new Type[] { Type.LONG_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }

    final InetSocketAddress serverAddress = new InetSocketAddress("localhost", 9901);

    final LinkedBlockingQueue<MessageWrapper> messageBuffer = new LinkedBlockingQueue<MessageWrapper>();
    ServerBootstrap acceptor = ParallelUtility.createMasterIPCServer(messageBuffer);
    Channel server = acceptor.bind(serverAddress);

    HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(0, new SocketInfo("localhost", 9901));

    final IPCConnectionPool connectionPool = new IPCConnectionPool(0, computingUnits);

    Thread[] threads = new Thread[numThreads];
    final AtomicInteger numSent = new AtomicInteger();
    for (int i = 0; i < numThreads; i++) {
      Thread tt = new Thread() {
        @Override
        public void run() {
          _TupleBatch tb = null;
          Iterator<TupleBatch> tbs = tbb.getAll().iterator();
          while (tbs.hasNext()) {
            tb = tbs.next();
            List<Column> columns = tb.outputRawData();
            final ColumnMessage[] columnProtos = new ColumnMessage[columns.size()];
            int j = 0;
            for (final Column c : columns) {
              columnProtos[j] = c.serializeToProto();
              j++;
            }

            TransportMessage tm =
                TransportMessage.newBuilder().setType(TransportMessageType.DATA).setData(
                    DataMessage.newBuilder().setType(DataMessageType.NORMAL).addAllColumns(Arrays.asList(columnProtos))
                        .setOperatorID(0l).build()).build();
            connectionPool.get(0, 3, null, null).write(tm);
            numSent.incrementAndGet();
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

    System.out.println("Total sent: " + numSent.get() + " TupleBatches");
    connectionPool.shutdown();
    server.close();
    server.disconnect();
    server.unbind().awaitUninterruptibly();
    Iterator<MessageWrapper> it = messageBuffer.iterator();
    int numReceived = 0;
    while (it.hasNext()) {
      MessageWrapper m = it.next();
      TransportMessage tm = m.message;
      if (tm.getType() == TransportMessage.TransportMessageType.DATA) {
        numReceived++;
      }
    }
    System.out.println("Received: " + numReceived + " TupleBatches");
    org.junit.Assert.assertEquals(numSent.get(), numReceived);
  }
}

package edu.washington.escience.myriad.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;
import edu.washington.escience.myriad.parallel.IPCConnectionPool;
import edu.washington.escience.myriad.parallel.ParallelUtility;
import edu.washington.escience.myriad.parallel.SocketInfo;
import edu.washington.escience.myriad.parallel.Worker.MessageWrapper;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage.DataMessageType;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage.TransportMessageType;
import edu.washington.escience.myriad.util.TestUtils;

public class NetworkTest {
  /** The logger for this class. Defaults to myriad.ipc level. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger("edu.washington.escience.myriad.ipc");

  public static class Sender {
    public static void main(String[] args) throws IOException, InterruptedException {

      int totalRestrict = TupleBatch.BATCH_SIZE;

      long[] ids = TestUtils.randomLong(1000, 1005, totalRestrict);
      long[] ids2 = TestUtils.randomLong(1000, 1005, totalRestrict);

      final Schema schema = new Schema(new Type[] { Type.LONG_TYPE, Type.LONG_TYPE }, new String[] { "id", "id2" });

      final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
      for (int i = 0; i < ids.length; i++) {
        tbb.put(0, ids[i]);
        tbb.put(1, ids2[i]);
      }
      final TupleBatch tb = tbb.popFilled();

      HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
      computingUnits.put(0, new SocketInfo(Receiver.addr.getHostString(), Receiver.addr.getPort()));

      final IPCConnectionPool connectionPool = new IPCConnectionPool(0, computingUnits, null);

      final AtomicInteger numSent = new AtomicInteger();
      long start = 0;
      long end = 0;

      Channel ch = connectionPool.get(0, 3, null);
      start = System.currentTimeMillis();
      System.out.println("Start at " + start);
      int numTB = 2100000000 / TupleBatch.BATCH_SIZE;
      System.out.println("Total num of TupleBatch: " + numTB);

      for (int i = 0; i < numTB; i++) {
        if (i % 100 == 0) {
          System.out.println(i + " sent");
        }
        List<Column<?>> columns = tb.outputRawData();
        final ColumnMessage[] columnProtos = new ColumnMessage[columns.size()];
        int j = 0;
        for (final Column<?> c : columns) {
          columnProtos[j] = c.serializeToProto();
          j++;
        }

        TransportMessage tm =
            TransportMessage.newBuilder().setType(TransportMessageType.DATA).setData(
                DataMessage.newBuilder().setType(DataMessageType.NORMAL).addAllColumns(Arrays.asList(columnProtos))
                    .setOperatorID(0l).build()).build();
        ch.write(tm);
        numSent.incrementAndGet();
      }
      ch.write(
          TransportMessage.newBuilder().setType(TransportMessageType.DATA).setData(
              DataMessage.newBuilder().setType(DataMessageType.EOS).setOperatorID(0l).build()).build())
          .awaitUninterruptibly();
      numSent.incrementAndGet();
      end = System.currentTimeMillis();
      System.out.println("Start at " + start);
      System.out.println("End at " + end);
      System.out.println("Time spent at receive: " + (end - start));

      LOGGER.debug("Total sent: " + numSent.get() + " TupleBatches");
      System.out.println();
      connectionPool.shutdown();
      System.exit(0);
    }
  }

  public static class Receiver {

    public static final InetSocketAddress addr = new InetSocketAddress("rio.cs.washington.edu", 19901);

    public static void main(String[] args) throws IOException, InterruptedException {

      final LinkedBlockingQueue<MessageWrapper> messageQueue = new LinkedBlockingQueue<MessageWrapper>();
      ServerBootstrap acceptor = ParallelUtility.createMasterIPCServer(messageQueue);
      Channel server = acceptor.bind(addr);

      int numReceived = 0;
      MessageWrapper m = null;

      long start = 0;
      long end = 0;

      RECEIVE_MESSAGE : while ((m = messageQueue.take()) != null) {

        TransportMessage tm = m.message;
        if (tm.getType() == TransportMessage.TransportMessageType.DATA) {
          if (numReceived % 100 == 0) {
            System.out.println(numReceived + " received");
          }
          final DataMessage data = tm.getData();
          switch (data.getType().getNumber()) {
            case DataMessageType.EOS_VALUE:
              end = System.currentTimeMillis();
              System.out.println("Receive start at " + start);
              System.out.println("Receive end at " + end);
              break RECEIVE_MESSAGE;
            case DataMessageType.NORMAL_VALUE:
              if (numReceived == 0) {
                start = System.currentTimeMillis();
                System.out.println("Receive start at new " + start);
              }
              final List<ColumnMessage> columnMessages = data.getColumnsList();
              final Column<?>[] columnArray = new Column[columnMessages.size()];
              int idx = 0;
              for (final ColumnMessage cm : columnMessages) {
                columnArray[idx++] = ColumnFactory.columnFromColumnMessage(cm);
              }
              numReceived += 1;
              break;
          }
        }
      }
      System.out.println("Total num received is " + numReceived);
      System.out.println("Time spent at receive: " + (end - start));
      server.close();
      server.disconnect();
      server.unbind().awaitUninterruptibly();
      server.getFactory().releaseExternalResources();
    }
  }
}

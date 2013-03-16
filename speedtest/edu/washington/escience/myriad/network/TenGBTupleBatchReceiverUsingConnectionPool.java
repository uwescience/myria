package edu.washington.escience.myriad.network;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;
import edu.washington.escience.myriad.parallel.SocketInfo;
import edu.washington.escience.myriad.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.util.QueueBasedMessageHandler.TestMessageWrapper;
import edu.washington.escience.myriad.util.TestUtils;

public class TenGBTupleBatchReceiverUsingConnectionPool {

  public static final int PORT = 19901;

  public static void main(final String[] args) throws Exception {

    final String senderHostName = args[0];

    final LinkedBlockingQueue<TestMessageWrapper> messageQueue = new LinkedBlockingQueue<TestMessageWrapper>();
    final HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(0, new SocketInfo(InetAddress.getLocalHost().getHostName(), PORT));
    computingUnits.put(1, new SocketInfo(senderHostName, TenGBTupleBatchSenderUsingConnectionPool.PORT));

    final IPCConnectionPool connectionPool = TestUtils.startIPCConnectionPool(0, computingUnits, messageQueue);

    long numReceived = 0;
    TestMessageWrapper m = null;

    long start = 0;
    long end = 0;
    final TupleBatchBuffer tbb = new TupleBatchBuffer(TenGBTupleBatchSenderUsingConnectionPool.schema);

    RECEIVE_MESSAGE : while ((m = messageQueue.take()) != null) {
      final TransportMessage tm = m.message;
      if (tm.getType() == TransportMessage.TransportMessageType.DATA) {
        if (numReceived % 100 == 0) {
          System.out.println(numReceived + " received");
        }
        final DataMessage data = tm.getData();
        switch (data.getType()) {
          case EOS:
            end = System.currentTimeMillis();
            System.out.println("Receive start at " + start);
            System.out.println("Receive end at " + end);
            break RECEIVE_MESSAGE;
          case EOI:
            // nothing to do
            break RECEIVE_MESSAGE;
          case NORMAL:
            if (numReceived == 0) {
              start = System.currentTimeMillis();
              System.out.println("Receive start at new " + start);
            }
            final List<ColumnMessage> columnMessages = data.getColumnsList();
            final Column<?>[] columnArray = new Column[columnMessages.size()];
            int idx = 0;
            for (final ColumnMessage cm : columnMessages) {
              columnArray[idx++] = ColumnFactory.columnFromColumnMessage(cm, data.getNumTuples());
            }
            final TupleBatch tb =
                new TupleBatch(TenGBTupleBatchSenderUsingConnectionPool.schema, Arrays.asList(columnArray),
                    columnArray[0].size(), -1, -1);
            tb.compactInto(tbb);
            while (tbb.popAny() != null) {
            }

            numReceived += 1;
            break;
        }
      }
    }
    System.out.println("Total num received is " + numReceived);
    System.out.println("Time spent at receive: " + (end - start));
    if (tbb.numTuples() > 1000) {
      System.out.println("Just to make use of tbb so that java won't do smart optimizations");
    }
    connectionPool.shutdown().await();
  }
}

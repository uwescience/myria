package edu.washington.escience.myriad.network;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;
import edu.washington.escience.myriad.parallel.IPCConnectionPool;
import edu.washington.escience.myriad.parallel.SocketInfo;
import edu.washington.escience.myriad.parallel.Worker.MessageWrapper;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage.DataMessageType;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;

public class TenGBTupleBatchReceiverUsingConnectionPool {

  public static final int PORT = 19901;

  public static void main(final String[] args) throws IOException, InterruptedException {

    final String senderHostName = args[0];

    final LinkedBlockingQueue<MessageWrapper> messageQueue = new LinkedBlockingQueue<MessageWrapper>();
    final HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(0, new SocketInfo(InetAddress.getLocalHost().getHostName(), PORT));
    computingUnits.put(1, new SocketInfo(senderHostName, TenGBTupleBatchSenderUsingConnectionPool.PORT));

    final IPCConnectionPool connectionPool = new IPCConnectionPool(0, computingUnits, messageQueue);

    connectionPool.start();

    long numReceived = 0;
    MessageWrapper m = null;

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
            final TupleBatch tb =
                new TupleBatch(TenGBTupleBatchSenderUsingConnectionPool.schema, Arrays.asList(columnArray),
                    columnArray[0].size());
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
    connectionPool.shutdown().awaitUninterruptibly();
  }
}

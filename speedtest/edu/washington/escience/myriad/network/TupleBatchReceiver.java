package edu.washington.escience.myriad.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;

import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;
import edu.washington.escience.myriad.parallel.ParallelUtility;
import edu.washington.escience.myriad.parallel.Worker.MessageWrapper;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage.DataMessageType;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;

public class TupleBatchReceiver {

  public static InetSocketAddress addr;

  public static void main(String[] args) throws IOException, InterruptedException {

    String hostName = args[0];
    int port = Integer.valueOf(args[1]);
    addr = new InetSocketAddress(hostName, port);

    final LinkedBlockingQueue<MessageWrapper> messageQueue = new LinkedBlockingQueue<MessageWrapper>();
    ServerBootstrap acceptor = ParallelUtility.createMasterIPCServer(messageQueue);
    Channel server = acceptor.bind(addr);

    long numReceived = 0;
    MessageWrapper m = null;

    long start = 0;
    long end = 0;
    TupleBatchBuffer tbb = new TupleBatchBuffer(TupleBatchSender.schema);

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
            TupleBatch tb = new TupleBatch(TupleBatchSender.schema, Arrays.asList(columnArray), columnArray[0].size());
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
    server.close();
    server.disconnect();
    server.unbind().awaitUninterruptibly();
    server.getFactory().releaseExternalResources();
  }
}

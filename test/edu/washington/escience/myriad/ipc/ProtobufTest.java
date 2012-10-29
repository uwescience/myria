package edu.washington.escience.myriad.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.junit.Test;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.parallel.IPCConnectionPool;
import edu.washington.escience.myriad.parallel.ParallelUtility;
import edu.washington.escience.myriad.parallel.SocketInfo;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage.DataMessageType;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage.TransportMessageType;
import edu.washington.escience.myriad.systemtest.SystemTestBase;
import edu.washington.escience.myriad.table._TupleBatch;

public class ProtobufTest {

  public class TestHandler extends IoHandlerAdapter {

    @Override
    public final void exceptionCaught(final IoSession session, final Throwable cause) {
      // System.out.println("exception caught");
      // cause.printStackTrace();
      System.out.print(cause.getMessage());
    }

    /**
     * Got called every time a message is received.
     */
    @Override
    public final void messageReceived(final IoSession session, final Object message) {
      // System.out.println("Message received: " + message);
      // printed++;
      // if (printed % 100 == 0) {
      // System.out.println();
      // }
      // System.out.print(".");
      //
      synchronized (lock) {
        printed++;
        System.out.println("Current received: " + printed);
      }
    }

    int printed = 0;
    ArrayList lock = new ArrayList(0);
  }

  @Test
  public void protobufTest() throws IOException {

    String[] names = SystemTestBase.randomFixedLengthNumericString(1000, 1005, 200000, 20);
    long[] ids = SystemTestBase.randomLong(1000, 1005, names.length);

    final Schema schema = new Schema(new Type[] { Type.LONG_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });

    TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }

    _TupleBatch tb = null;
    InetSocketAddress serverAddress = new InetSocketAddress("localhost", 9901);
    TestHandler handler = new TestHandler();

    NioSocketAcceptor acceptor = ParallelUtility.createAcceptor();
    acceptor.setHandler(handler);
    acceptor.bind(serverAddress);
    // IoSession s = ParallelUtility.createSession(serverAddress, handler, 1000);

    HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(0, new SocketInfo("localhost", 9901));
    HashMap<Integer, IoHandler> handlers = new HashMap<Integer, IoHandler>();
    handlers.put(0, handler);
    IPCConnectionPool connectionPool = new IPCConnectionPool(0, computingUnits, handlers);

    while ((tb = tbb.popAny()) != null) {
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
      IoSession s = connectionPool.get(0, null, 3, null);
      s.write(tm);
    }

  }

  @Test
  public void protobufMultiThreadTest() throws IOException, InterruptedException {

    String[] names = SystemTestBase.randomFixedLengthNumericString(1000, 1005, 20000, 20);
    long[] ids = SystemTestBase.randomLong(1000, 1005, names.length);

    final Schema schema = new Schema(new Type[] { Type.LONG_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });

    int numThreads = 20;

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }

    final InetSocketAddress serverAddress = new InetSocketAddress("localhost", 9901);
    final TestHandler handler = new TestHandler();

    NioSocketAcceptor acceptor = ParallelUtility.createAcceptor();
    acceptor.setHandler(handler);
    acceptor.bind(serverAddress);
    HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(0, new SocketInfo("localhost", 9901));
    // HashMap<Integer, IoHandler> handlers = new HashMap<Integer, IoHandler>();
    // handlers.put(0, handler);
    // final IPCConnectionPool connectionPool = new IPCConnectionPool(0, computingUnits, handlers);
    // final ArrayList lock = new ArrayList();
    // final IoSession initialSession = connectionPool.get(0, null, 3, null);
    Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      final int t = i;
      Thread tt = new Thread() {
        @Override
        public void run() {
          // final IoSession initialSession = connectionPool.get(0, null, 3, null);
          int sent = 0;
          IoSession initialSession = ParallelUtility.createSession(serverAddress, handler, 1000);
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
            // synchronized (lock) {
            // IoSession nowSession = connectionPool.get(0, null, 3, null);
            // if (nowSession != initialSession) {
            // System.err.println("Not the same session!");
            // }
            // System.out.print("-");
            initialSession.write(tm);
            sent++;
            // }
            // System.out.println("Thread#" + t + " sent a TM");
          }
          initialSession.close(false).awaitUninterruptibly();
          System.out.println("sent " + sent);
        }
      };
      threads[i] = tt;
      tt.setDaemon(false);
      tt.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    acceptor.dispose(true);

  }
}

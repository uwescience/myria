package edu.washington.escience.myriad.network;

import java.net.InetAddress;
import java.util.HashMap;

import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.parallel.SocketInfo;
import edu.washington.escience.myriad.parallel.TransportMessageSerializer;
import edu.washington.escience.myriad.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myriad.parallel.ipc.IPCMessage;
import edu.washington.escience.myriad.parallel.ipc.SimpleBagInputBuffer;
import edu.washington.escience.myriad.parallel.ipc.StreamIOChannelID;
import edu.washington.escience.myriad.util.TestUtils;

public class TenGBTupleBatchReceiverUsingConnectionPool {

  public static final int PORT = 19901;

  public static final int IPCID = 0;

  public static void main(final String[] args) throws Exception {

    final String senderHostName = args[0];

    final HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(IPCID, new SocketInfo(InetAddress.getLocalHost().getHostName(), PORT));
    computingUnits.put(TenGBTupleBatchSenderUsingConnectionPool.IPCID, new SocketInfo(senderHostName,
        TenGBTupleBatchSenderUsingConnectionPool.PORT));

    final IPCConnectionPool connectionPool =
        TestUtils.startIPCConnectionPool(IPCID, computingUnits, null, new TransportMessageSerializer());
    ImmutableSet.Builder<StreamIOChannelID> sourceChannelSetBuilder = ImmutableSet.builder();
    SimpleBagInputBuffer<TupleBatch> sib =
        new SimpleBagInputBuffer<TupleBatch>(connectionPool, sourceChannelSetBuilder.add(
            new StreamIOChannelID(TenGBTupleBatchSenderUsingConnectionPool.streamID,
                TenGBTupleBatchSenderUsingConnectionPool.IPCID)).build());
    sib.setAttachment(TenGBTupleBatchSenderUsingConnectionPool.schema);
    sib.start(TenGBTupleBatchReceiverUsingConnectionPool.class);

    long numReceived = 0;
    IPCMessage.Data<TupleBatch> m = null;

    long start = 0;
    long end = 0;
    final TupleBatchBuffer tbb = new TupleBatchBuffer(TenGBTupleBatchSenderUsingConnectionPool.schema);

    while (!sib.isEmpty() || !sib.isEOS()) {
      m = sib.take();
      final TupleBatch tm = m.getPayload();
      if (tm != null) {
        if (numReceived % 100 == 0) {
          System.out.println(numReceived + " received");
        }
        tm.compactInto(tbb);
        while (tbb.popAny() != null) {
          /* Pass -- emptying tuple buffer. */
        }

        numReceived += 1;
      }

    }
    System.out.println("Receive start at " + start);
    System.out.println("Receive end at " + end);
    System.out.println("Total num received is " + numReceived);
    System.out.println("Time spent at receive: " + (end - start));
    if (tbb.numTuples() > 1000) {
      System.out.println("Just to make use of tbb so that java won't do smart optimizations");
    }
    connectionPool.shutdown().await();
  }
}
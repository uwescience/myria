package edu.washington.escience.myria.network;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.jboss.netty.channel.ChannelFuture;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.ipc.IPCTestUtil;
import edu.washington.escience.myria.parallel.SocketInfo;
import edu.washington.escience.myria.parallel.TransportMessageSerializer;
import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myria.parallel.ipc.IPCMessage;
import edu.washington.escience.myria.parallel.ipc.StreamOutputChannel;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestUtils;

public class TenGBTupleBatchSenderUsingConnectionPool {

  public static final int PORT = 19902;

  final static Schema schema =
      new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("id", "id2"));

  public static final int IPCID = 1;

  public static final long streamID = 1;

  public static void doSend(
      final String[] args, final TupleBatchBuffer dataToSend, final int tupleSize)
      throws Exception {

    final String receiveHostName = args[0];

    final HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(
        TenGBTupleBatchReceiverUsingConnectionPool.IPCID,
        new SocketInfo(receiveHostName, TenGBTupleBatchReceiverUsingConnectionPool.PORT));
    computingUnits.put(IPCID, new SocketInfo(InetAddress.getLocalHost().getHostName(), PORT));

    final IPCConnectionPool connectionPool =
        IPCTestUtil.startIPCConnectionPool(
            1,
            computingUnits,
            new LinkedBlockingQueue<IPCMessage.Data<TupleBatch>>(),
            new TransportMessageSerializer(),
            10,
            8,
            Runtime.getRuntime().availableProcessors() * 2 + 1);

    long numSent = 0;
    long start = 0;
    long end = 0;

    final StreamOutputChannel<TupleBatch> ch =
        Preconditions.checkNotNull(
            connectionPool.<TupleBatch>reserveLongTermConnection(
                TenGBTupleBatchReceiverUsingConnectionPool.IPCID, streamID));
    start = System.currentTimeMillis();
    System.out.println("Start at " + start);

    final TupleBatch tm = dataToSend.popAny();
    final long serializedSize = tm.toTransportMessage().getSerializedSize();
    System.out.println(
        "TupleBatch payload size: " + ((Long.SIZE / 8) + tupleSize * TupleBatch.BATCH_SIZE));
    System.out.println("TupleBatch serialized size: " + serializedSize);
    final long tenGBytes = 10L * 1024L * 1024L * 1024L;
    System.out.println("Total bytes to send: " + tenGBytes);

    final long numTB = tenGBytes / serializedSize;
    final long realSentSize = numTB * serializedSize;
    System.out.println("Total num of TupleBatch: " + numTB);
    System.out.println("Real sent size: " + realSentSize * 1.0 / 1024 / 1024 / 1024 + "GBytes");

    ChannelFuture cf = null;
    for (int i = 0; i < numTB; i++) {
      if (i % 100 == 0) {
        if (cf != null) {
          cf.await();
        }
        System.out.println(i + " sent");
        System.out.println(
            "Current Speed: "
                + serializedSize * 1.0 * numSent * 1.0 / 1024 / 1024 / elapsedInSeconds(start)
                + " mega-bytes/s");
      }
      cf = ch.write(tm);
      numSent++;
    }
    ch.release().await();
    numSent++;
    end = System.currentTimeMillis();
    System.out.println("Start at " + start);
    System.out.println("End at " + end);
    System.out.println("Total sent: " + numSent + " TupleBatches");
    System.out.println("Total sent: " + realSentSize / 1024.0 / 1024.0 / 1024.0 + " G-bytes");
    System.out.println(
        "Total payload: "
            + numSent * tm.numTuples() * tupleSize / 1024.0 / 1024.0 / 1024.0
            + " G-bytes");
    System.out.println(
        "Speed: " + realSentSize * 1.0 / 1024 / 1024 / elapsedInSeconds(start) + "mega-bytes/s");

    System.out.println();
    connectionPool.shutdown().await();
    // System.exit(0);
  }

  public static double elapsedInSeconds(final long startTimeMS) {
    return (System.currentTimeMillis() - startTimeMS) * 1.0 / 1000;
  }

  public static void main(final String[] args) throws Exception {
    final int totalRestrict = TupleBatch.BATCH_SIZE;

    final long[] ids = TestUtils.randomLong(1000, 1005, totalRestrict);
    final long[] ids2 = TestUtils.randomLong(1000, 1005, totalRestrict);

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < ids.length; i++) {
      tbb.putLong(0, ids[i]);
      tbb.putLong(1, ids2[i]);
    }
    doSend(args, tbb, (Long.SIZE / 8) * 2);
  }
}

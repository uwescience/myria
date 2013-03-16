package edu.washington.escience.myriad.network;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.parallel.SocketInfo;
import edu.washington.escience.myriad.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.util.IPCUtils;
import edu.washington.escience.myriad.util.QueueBasedMessageHandler.TestMessageWrapper;
import edu.washington.escience.myriad.util.TestUtils;

public class TenGBTupleBatchSenderUsingConnectionPool {

  public static final int PORT = 19902;

  final static Schema schema = new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("id",
      "id2"));

  public static void doSend(final String[] args, final TupleBatchBuffer dataToSend, final int tupleSize)
      throws Exception {

    final String receiveHostName = args[0];

    final HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(0, new SocketInfo(receiveHostName, TenGBTupleBatchReceiverUsingConnectionPool.PORT));
    computingUnits.put(1, new SocketInfo(InetAddress.getLocalHost().getHostName(), PORT));

    final IPCConnectionPool connectionPool =
        TestUtils.startIPCConnectionPool(1, computingUnits, new LinkedBlockingQueue<TestMessageWrapper>());

    long numSent = 0;
    long start = 0;
    long end = 0;

    final ExchangePairID eID = ExchangePairID.fromExisting(0L);
    final Channel ch = connectionPool.reserveLongTermConnection(0);
    ch.write(IPCUtils.bosTM(eID));
    start = System.currentTimeMillis();
    System.out.println("Start at " + start);

    final TransportMessage tm = dataToSend.popAnyAsTM(0);
    final long serializedSize = tm.getSerializedSize();
    System.out.println("TupleBatch payload size: " + ((Long.SIZE / 8) + tupleSize * TupleBatch.BATCH_SIZE));
    System.out.println("TupleBatch serialized size: " + serializedSize);
    final long tenGBytes = 10l * 1024l * 1024l * 1024l;
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
        System.out.println("Current Speed: " + serializedSize * 1.0 * numSent * 1.0 / 1024 / 1024
            / elapsedInSeconds(start) + " mega-bytes/s");
      }
      cf = ch.write(tm);
      numSent++;
    }
    ch.write(IPCUtils.EOS).await();
    connectionPool.releaseLongTermConnection(ch);
    numSent++;
    end = System.currentTimeMillis();
    System.out.println("Start at " + start);
    System.out.println("End at " + end);
    System.out.println("Total sent: " + numSent + " TupleBatches");
    System.out.println("Total sent: " + realSentSize / 1024.0 / 1024.0 / 1024.0 + " G-bytes");
    System.out.println("Total payload: " + numSent * tm.getData().getNumTuples() * tupleSize / 1024.0 / 1024.0 / 1024.0
        + " G-bytes");
    System.out.println("Speed: " + realSentSize * 1.0 / 1024 / 1024 / elapsedInSeconds(start) + "mega-bytes/s");

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
      tbb.put(0, ids[i]);
      tbb.put(1, ids2[i]);
    }
    doSend(args, tbb, (Long.SIZE / 8) * 2);
  }
}

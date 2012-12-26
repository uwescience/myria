package edu.washington.escience.myriad.network;

import java.io.IOException;
import java.util.HashMap;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.parallel.IPCConnectionPool;
import edu.washington.escience.myriad.parallel.SocketInfo;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.util.IPCUtils;
import edu.washington.escience.myriad.util.TestUtils;

public class TupleBatchSender {
  public static double elapsedInSeconds(long startTimeMS) {
    return (System.currentTimeMillis() - startTimeMS) * 1.0 / 1000;
  }

  final static Schema schema = new Schema(new Type[] { Type.LONG_TYPE, Type.LONG_TYPE }, new String[] { "id", "id2" });

  public static void main(String[] args) throws IOException, InterruptedException {

    String hostName = args[0];
    int port = Integer.valueOf(args[1]);

    int totalRestrict = TupleBatch.BATCH_SIZE;

    long[] ids = TestUtils.randomLong(1000, 1005, totalRestrict);
    long[] ids2 = TestUtils.randomLong(1000, 1005, totalRestrict);

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < ids.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, ids2[i]);
    }

    HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(0, new SocketInfo(hostName, port));

    final IPCConnectionPool connectionPool = new IPCConnectionPool(0, computingUnits, null);

    long numSent = 0;
    long start = 0;
    long end = 0;

    Channel ch = connectionPool.get(0, 3, null);
    start = System.currentTimeMillis();
    System.out.println("Start at " + start);

    ExchangePairID eID = ExchangePairID.fromExisting(0L);
    final TransportMessage tm = tbb.popAnyAsTM(eID);// popFilledAsRawColumn();
    long serializedSize = tm.getSerializedSize();
    System.out.println("TupleBatch payload size: " + ((Long.SIZE / 8) + (Long.SIZE / 8) * 2 * TupleBatch.BATCH_SIZE));
    System.out.println("TupleBatch serialized size: " + serializedSize);
    long tenGBytes = 10l * 1024l * 1024l * 1024l;
    System.out.println("Total bytes to send: " + tenGBytes);

    long numTB = tenGBytes / serializedSize;
    long realSentSize = numTB * serializedSize;
    System.out.println("Total num of TupleBatch: " + numTB);
    System.out.println("Real sent size: " + realSentSize * 1.0 / 1024 / 1024 / 1024 + "GBytes");

    ChannelFuture cf = null;
    for (int i = 0; i < numTB; i++) {
      if (i % 100 == 0) {
        if (cf != null) {
          cf.awaitUninterruptibly();
        }
        System.out.println(i + " sent");
        System.out.println("Current Speed: " + serializedSize * 1.0 * numSent * 1.0 / 1024 / 1024
            / elapsedInSeconds(start) + " mega-bytes/s");
      }
      cf = ch.write(tm);
      numSent++;
    }
    ch.write(IPCUtils.eosTM(eID)).awaitUninterruptibly();
    numSent++;
    end = System.currentTimeMillis();
    System.out.println("Start at " + start);
    System.out.println("End at " + end);
    System.out.println("Total sent: " + numSent + " TupleBatches");
    System.out.println("Speed: " + realSentSize * 1.0 / 1024 / 1024 / elapsedInSeconds(start) + "mega-bytes/s");

    System.out.println();
    connectionPool.shutdown();
    System.exit(0);
  }
}

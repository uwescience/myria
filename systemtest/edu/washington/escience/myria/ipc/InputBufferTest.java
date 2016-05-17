package edu.washington.escience.myria.ipc;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.AbstractChannel;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroupFuture;
import org.jboss.netty.channel.socket.nio.NioSocketChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;

import edu.washington.escience.myria.parallel.SocketInfo;
import edu.washington.escience.myria.parallel.TransportMessageSerializer;
import edu.washington.escience.myria.parallel.ipc.ChannelContext;
import edu.washington.escience.myria.parallel.ipc.FlowControlBagInputBuffer;
import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myria.parallel.ipc.IPCMessage.StreamData;
import edu.washington.escience.myria.parallel.ipc.InJVMChannel;
import edu.washington.escience.myria.parallel.ipc.StreamIOChannelID;
import edu.washington.escience.myria.parallel.ipc.StreamInputBuffer;
import edu.washington.escience.myria.parallel.ipc.StreamInputChannel;
import edu.washington.escience.myria.parallel.ipc.StreamOutputChannel;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.IPCUtils;
import edu.washington.escience.myria.util.TestUtils;

public class InputBufferTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(InputBufferTest.class);

  final int NUM_PSEUDO_WORKER = 10;
  final int NUM_STREAM = 20;
  final int NUM_MSGS_PER_INPUT = 100;

  IPCConnectionPool[] pools = null;
  final TupleBatch tb =
      TestUtils.generateRandomTuples(TupleBatch.BATCH_SIZE, TupleBatch.BATCH_SIZE, false).popAny();;

  @Before
  public void init() throws Throwable {
    final HashMap<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();

    for (int i = 0; i < NUM_PSEUDO_WORKER; i++) {
      computingUnits.put(i, SocketInfo.valueOf("localhost:" + (9000 + i)));
    }

    pools = new IPCConnectionPool[NUM_PSEUDO_WORKER];
    for (int i = 0; i < pools.length; i++) {
      pools[i] =
          IPCTestUtil.startIPCConnectionPool(
              i, computingUnits, null, new TransportMessageSerializer(), 2, 1, 5);
    }
  }

  @After
  public void cleanup() {
    for (IPCConnectionPool p : pools) {
      p.shutdownNow().awaitUninterruptibly();
      p.releaseExternalResources();
    }
  }

  @Test
  public void flowControlInputBufferCollectTest() throws Throwable {
    try {

      Random r = new Random();
      // while (true) {
      int receiverID = r.nextInt(NUM_PSEUDO_WORKER);
      int numWorker = Math.max(r.nextInt(NUM_PSEUDO_WORKER), 1);
      int numStream = Math.max(r.nextInt(NUM_STREAM), 1);
      int numMsgs = Math.max(r.nextInt(NUM_MSGS_PER_INPUT), 10);

      Builder<StreamIOChannelID> builder = ImmutableSet.<StreamIOChannelID>builder();
      ImmutableSet<StreamIOChannelID> channelIDs = null;
      for (int i = 0; i < numWorker; i++) {
        for (int j = 0; j < numStream; j++) {
          builder.add(new StreamIOChannelID(j, i));
        }
      }
      channelIDs = builder.build();
      System.out.println(
          "Starting r:" + receiverID + "#W:" + numWorker + "#S:" + numStream + "#Msg:" + numMsgs);
      doCollectTest(
          receiverID,
          numWorker,
          numStream,
          numMsgs,
          new FlowControlBagInputBuffer<TupleBatch>(pools[receiverID], channelIDs, 2, 1));
      // }
    } catch (Throwable e) {
      LOGGER.error("Test error", e);
      throw e;
    }
  }

  public void doCollectTest(
      final int collectorID,
      final int numWorker,
      final int numStreamPerWorker,
      final int numMsgsPerInput,
      final StreamInputBuffer<TupleBatch> ib)
      throws Throwable {

    ib.setAttachment(tb.getSchema());

    final ArrayList<StreamInputChannel<TupleBatch>> ics = new ArrayList<>();
    for (StreamIOChannelID id : ib.getSourceChannels()) {
      ics.add(ib.getInputChannel(id));
    }
    ib.start("Test input buffer");

    ArrayList<Thread> inputThreads = new ArrayList<Thread>(numWorker * numStreamPerWorker);

    for (int i = 0; i < numWorker; i++) {
      final int j = i;
      for (int k = 0; k < numStreamPerWorker; k++) {
        final long sID = k;
        final StreamOutputChannel<TupleBatch> out =
            pools[j].<TupleBatch>reserveLongTermConnection(collectorID, sID);
        inputThreads.add(
            new Thread(
                new Runnable() {
                  @Override
                  public void run() {
                    for (int i = 0; i < numMsgsPerInput; i++) {
                      while (true) {
                        if (out.isWritable()) {
                          out.write(
                              IPCUtils.tmToTupleBatch(
                                  tb.toTransportMessage().getDataMessage(), tb.getSchema()));
                          break;
                        }
                      }
                    }
                    out.release();
                  }
                }));
      }
    }

    for (Thread inputThread : inputThreads) {
      inputThread.start();
    }

    long numReceived = 0;
    int numTB = 0;
    while (!ib.isEOS() || !ib.isEmpty()) {
      StreamData<TupleBatch> data = ib.take();
      numTB++;
      if (numTB % 2 == 0) {
        System.out.println(".");
      } else {
        System.out.println("-");
      }
      TupleBatch ttbb = data.getPayload();
      if (ttbb == null) {
      } else {
        numReceived += ttbb.numTuples();
      }
    }

    pools[collectorID].deRegisterStreamInput(ib);

    assertEquals(numWorker * numStreamPerWorker * numMsgsPerInput * tb.numTuples(), numReceived);
    assertAllChannelReadable();
    System.out.println(".");
  }

  public void assertAllChannelReadable() throws Throwable {
    Field f = AbstractChannel.class.getDeclaredField("allChannels");
    f.setAccessible(true);
    @SuppressWarnings("unchecked")
    ConcurrentMap<Integer, Channel> allChannels = (ConcurrentMap<Integer, Channel>) f.get(null);

    LinkedList<ChannelFuture> allFutures = new LinkedList<ChannelFuture>();
    ChannelGroup cg = new DefaultChannelGroup();

    for (Map.Entry<Integer, Channel> entry : allChannels.entrySet()) {
      final Channel ch = entry.getValue();
      final ChannelFuture cf = Channels.future(ch);

      if (ch instanceof NioSocketChannel || ch instanceof InJVMChannel) {
        allFutures.add(cf);
        cg.add(ch);
        ch.getPipeline()
            .execute(
                new Runnable() {
                  @Override
                  public void run() {
                    if (!ch.isReadable()) {
                      cf.setFailure(
                          new IllegalStateException(
                              "Channel "
                                  + ChannelContext.channelToString(ch)
                                  + " is not readable"));
                    } else {
                      cf.setSuccess();
                    }
                  }
                });
      }
    }
    ChannelGroupFuture cgf = new DefaultChannelGroupFuture(cg, allFutures);
    cgf.awaitUninterruptibly(10, TimeUnit.SECONDS);
    if (!cgf.isCompleteSuccess()) {
      for (ChannelFuture cf : cgf) {
        if (!cf.isSuccess()) {
          throw cf.getCause();
        }
      }
    }
  }
}

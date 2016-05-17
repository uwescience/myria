package edu.washington.escience.myria.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ByteBufferBackedChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.compression.ZlibDecoder;
import org.jboss.netty.handler.codec.compression.ZlibEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestUtils;

public class TenGBCompressSender {

  public static class ClientPipelineFactory implements ChannelPipelineFactory {
    /**
     * constructor.
     * */
    ClientPipelineFactory() {}

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline p = Channels.pipeline();
      p.addLast("compressionDecoder", new ZlibDecoder()); // upstream 1
      p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder()); // upstream 2
      // p.addLast("protobufDecoder", protobufDecoder); // upstream 3

      p.addLast("compressionEncoder", new ZlibEncoder(2)); // downstream 1
      p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender()); // downstream 2
      // p.addLast("protobufEncoder", protobufEncoder); // downstream 3

      return p;
    }
  }

  final static Schema schema =
      new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("id", "id2"));

  public static double elapsedInSeconds(final long startTimeMS) {
    return (System.currentTimeMillis() - startTimeMS) * 1.0 / 1000;
  }

  public static void main(final String[] args) throws IOException, InterruptedException {

    final String hostName = args[0];
    final int port = Integer.valueOf(args[1]);

    final int totalRestrict = TupleBatch.BATCH_SIZE;

    final long[] ids = TestUtils.randomLong(1000, 1005, totalRestrict);
    final long[] ids2 = TestUtils.randomLong(1000, 1005, totalRestrict);

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < ids.length; i++) {
      tbb.putLong(0, ids[i]);
      tbb.putLong(1, ids2[i]);
    }

    final InetSocketAddress remoteAddress = new InetSocketAddress(hostName, port);

    final ClientBootstrap bootstrap =
        new ClientBootstrap(
            new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
    bootstrap.setPipelineFactory(new ClientPipelineFactory());
    bootstrap.setOption("tcpNoDelay", true);
    bootstrap.setOption("keepAlive", false);
    bootstrap.setOption("reuseAddress", true);
    bootstrap.setOption("connectTimeoutMillis", 3000);
    bootstrap.setOption("sendBufferSize", 512 * 1024);
    bootstrap.setOption("receiveBufferSize", 512 * 1024);
    bootstrap.setOption("writeBufferLowWaterMark", 16 * 1024);
    bootstrap.setOption("writeBufferHighWaterMark", 256 * 1024);

    ChannelFuture c = bootstrap.connect(remoteAddress);
    if (!c.awaitUninterruptibly().isSuccess()) {
      throw new RuntimeException("Unable to connect");
    }
    Channel ch = c.getChannel();

    long numSent = 0;
    long start = 0;
    final long end = 0;

    start = System.currentTimeMillis();
    System.out.println("Start at " + start);

    final long tenGBytes = 10L * 1024L * 1024L * 1024L;
    final byte[] buffer = new byte[512 * 1024];
    final ChannelBuffer cb = new ByteBufferBackedChannelBuffer(ByteBuffer.wrap(buffer));

    System.out.println("Total bytes to send: " + tenGBytes);

    final long numBlocks = tenGBytes / buffer.length;
    System.out.println("Total num of 512kb-size blocks: " + numBlocks);

    ChannelFuture cf = null;
    for (int i = 0; i < numBlocks; i++) {
      if (i % 100 == 0) {
        if (cf != null) {
          cf.awaitUninterruptibly();
        }
        System.out.println(i + " sent");
        System.out.println(
            "Current Speed: "
                + buffer.length * 1.0 * numSent * 1.0 / 1024 / 1024 / elapsedInSeconds(start)
                + " mega-bytes/s");
      }
      cf = ch.write(cb.duplicate());
      numSent++;
    }
    ch.write(new ByteBufferBackedChannelBuffer(ByteBuffer.wrap(new byte[] {0})))
        .awaitUninterruptibly();
    numSent++;
    System.out.println("Start at " + start);
    System.out.println("End at " + end);
    System.out.println("Total sent: " + numSent + " 512kb-size blocks");
    System.out.println("Time spent at receive: " + elapsedInSeconds(start) + " seconds");
    System.out.println(
        "Speed: " + tenGBytes * 1.0 / 1024 / 1024 / elapsedInSeconds(start) + "mega-bytes/s");

    System.out.println();
    System.exit(0);
  }
}

package edu.washington.escience.myria.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

public class TenGBReceiver {

  public static class DataHandler extends SimpleChannelUpstreamHandler {

    final LinkedBlockingQueue<Object> dataQueue;

    /**
     * constructor.
     * */
    public DataHandler(final LinkedBlockingQueue<Object> dataQueue) {
      this.dataQueue = dataQueue;
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final ExceptionEvent e) {
      e.getCause().printStackTrace();
      e.getChannel().close();
    }

    @Override
    public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) {
      dataQueue.add(e.getMessage());

      ctx.sendUpstream(e);
    }
  }

  public static class ServerPipelineFactory implements ChannelPipelineFactory {

    final LinkedBlockingQueue<Object> dataQueue;

    /**
     * constructor.
     * */
    public ServerPipelineFactory(final LinkedBlockingQueue<Object> dataQueue) {
      this.dataQueue = dataQueue;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline p = Channels.pipeline();
      p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder()); // upstream 2

      p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender()); // downstream 2

      p.addLast("handler", new DataHandler(dataQueue));
      return p;
    }
  }

  public static InetSocketAddress addr;

  public static void main(final String[] args) throws IOException, InterruptedException {

    final String hostName = args[0];
    final int port = Integer.valueOf(args[1]);
    addr = new InetSocketAddress(hostName, port);

    // Start server with Nb of active threads = 2*NB CPU + 1 as maximum.
    final ChannelFactory factory =
        new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool(),
            Runtime.getRuntime().availableProcessors() * 2 + 1);

    final ServerBootstrap bootstrap = new ServerBootstrap(factory);

    final LinkedBlockingQueue<Object> dataQueue = new LinkedBlockingQueue<Object>();
    bootstrap.setPipelineFactory(new ServerPipelineFactory(dataQueue));

    bootstrap.setOption("child.tcpNoDelay", true);
    bootstrap.setOption("child.keepAlive", false);
    bootstrap.setOption("child.reuseAddress", true);
    bootstrap.setOption("child.connectTimeoutMillis", 3000);
    bootstrap.setOption("child.sendBufferSize", 512 * 1024);
    bootstrap.setOption("child.receiveBufferSize", 512 * 1024);
    bootstrap.setOption("child.writeBufferLowWaterMark", 16 * 1024);
    bootstrap.setOption("child.writeBufferHighWaterMark", 256 * 1024);

    bootstrap.setOption("readWriteFair", true);

    final Channel server = bootstrap.bind(addr);

    long numReceived = 0;
    Object m = null;

    final long start = 0;

    while ((m = dataQueue.take()) != null) {
      if (m instanceof byte[]) {
        final byte[] data = (byte[]) m;
        System.out.println("received data block, length: " + data.length);
        if (data.length == 1) {
          break;
        }
      } else if (m instanceof ChannelBuffer) {
        final ChannelBuffer cb = (ChannelBuffer) m;
        System.out.println("received data block, length: " + cb.capacity());
        if (cb.capacity() == 1) {
          break;
        }
      } else {
        System.out.println("Object received, with type: " + m.getClass().getCanonicalName());
      }
      numReceived++;
    }
    System.out.println("Total num received is " + numReceived);
    System.out.println(
        "Time spent at receive: "
            + TenGBTupleBatchSenderUsingConnectionPool.elapsedInSeconds(start)
            + " seconds");

    server.close();
    server.disconnect();
    server.unbind().awaitUninterruptibly();
    server.getFactory().releaseExternalResources();
  }
}

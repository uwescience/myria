package edu.washington.escience.myriad.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
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

  public static InetSocketAddress addr;

  public static class ServerPipelineFactory implements ChannelPipelineFactory {

    /**
     * constructor.
     * */
    public ServerPipelineFactory(LinkedBlockingQueue<Object> dataQueue) {
      this.dataQueue = dataQueue;
    }

    final LinkedBlockingQueue<Object> dataQueue;

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      ChannelPipeline p = Channels.pipeline();
      p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder()); // upstream 2

      p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender()); // downstream 2

      p.addLast("handler", new DataHandler(dataQueue));
      return p;
    }
  }

  public static class DataHandler extends SimpleChannelUpstreamHandler {

    /**
     * constructor.
     * */
    public DataHandler(LinkedBlockingQueue<Object> dataQueue) {
      this.dataQueue = dataQueue;
    }

    final LinkedBlockingQueue<Object> dataQueue;

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
      // if (e instanceof ChannelStateEvent) {
      // logger.info(e.toString());
      // }
      super.handleUpstream(ctx, e);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      dataQueue.add(e.getMessage());

      ctx.sendUpstream(e);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      e.getCause().printStackTrace();
      e.getChannel().close();
    }

  }

  public static void main(String[] args) throws IOException, InterruptedException {

    String hostName = args[0];
    int port = Integer.valueOf(args[1]);
    addr = new InetSocketAddress(hostName, port);

    // Start server with Nb of active threads = 2*NB CPU + 1 as maximum.
    ChannelFactory factory =
        new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool(), Runtime
            .getRuntime().availableProcessors() * 2 + 1);

    ServerBootstrap bootstrap = new ServerBootstrap(factory);

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

    Channel server = bootstrap.bind(addr);

    long numReceived = 0;
    Object m = null;

    long start = 0;

    while ((m = dataQueue.take()) != null) {
      if (m instanceof byte[]) {
        byte[] data = (byte[]) m;
        System.out.println("received data block, length: " + data.length);
        if (data.length == 1) {
          break;
        }
      } else if (m instanceof ChannelBuffer) {
        ChannelBuffer cb = (ChannelBuffer) m;
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
    System.out.println("Time spent at receive: " + TenGBTupleBatchSenderUsingConnectionPool.elapsedInSeconds(start) + " seconds");

    server.close();
    server.disconnect();
    server.unbind().awaitUninterruptibly();
    server.getFactory().releaseExternalResources();
  }
}

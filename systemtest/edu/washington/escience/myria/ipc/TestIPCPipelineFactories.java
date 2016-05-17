package edu.washington.escience.myria.ipc;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myria.parallel.ipc.IPCMessageHandler;

public class TestIPCPipelineFactories {

  public static class InJVMPipelineFactory implements ChannelPipelineFactory {

    protected final IPCMessageHandler ipcMessageHandler;

    /**
     * constructor.
     * */
    public InJVMPipelineFactory(final IPCConnectionPool ipcConnectionPool) {
      ipcMessageHandler = new IPCMessageHandler(ipcConnectionPool);
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline p = Channels.pipeline();
      p.addLast("sm", ipcMessageHandler); // upstream 6

      return p;
    }
  }

  public static class ClientPipelineFactory implements ChannelPipelineFactory {
    protected final IPCMessageHandler ipcMessageHandler;

    /**
     * constructor.
     * */
    public ClientPipelineFactory(final IPCConnectionPool ipcConnectionPool) {
      ipcMessageHandler = new IPCMessageHandler(ipcConnectionPool);
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline p = Channels.pipeline();
      p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder()); // upstream 2
      p.addLast("frameEncoder", FRAME_ENCODER); // downstream 2
      p.addLast("ipcSessionManager", ipcMessageHandler); // upstream 5
      return p;
    }
  }

  public static class ServerPipelineFactory implements ChannelPipelineFactory {

    protected final IPCMessageHandler ipcSessionManager;

    /**
     * constructor.
     * */
    public ServerPipelineFactory(
        final IPCConnectionPool ipcConnectionPool,
        final OrderedMemoryAwareThreadPoolExecutor executor) {
      ipcSessionManager = new IPCMessageHandler(ipcConnectionPool);
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline p = Channels.pipeline();
      p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder()); // upstream 2
      p.addLast("frameEncoder", FRAME_ENCODER); // downstream 2
      p.addLast("ipcSessionManager", ipcSessionManager); // upstream 5
      return p;
    }
  }

  /**
   * separate data streams to data frames.
   * */
  static final ProtobufVarint32LengthFieldPrepender FRAME_ENCODER =
      new ProtobufVarint32LengthFieldPrepender();
}

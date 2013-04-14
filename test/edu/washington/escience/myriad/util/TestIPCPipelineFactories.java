package edu.washington.escience.myriad.util;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

import edu.washington.escience.myriad.parallel.IPCInputGuard;
import edu.washington.escience.myriad.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myriad.parallel.ipc.IPCSessionManagerClient;
import edu.washington.escience.myriad.parallel.ipc.IPCSessionManagerServer;
import edu.washington.escience.myriad.parallel.ipc.MessageChannelHandler;
import edu.washington.escience.myriad.proto.TransportProto;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;

public class TestIPCPipelineFactories {

  public static class InJVMPipelineFactory implements ChannelPipelineFactory {
    protected final MessageChannelHandler<TransportMessage> messageHandler;

    /**
     * constructor.
     * */
    public InJVMPipelineFactory(final MessageChannelHandler<TransportMessage> queueMessageHandler) {
      messageHandler = queueMessageHandler;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline p = Channels.pipeline();
      p.addLast("inputVerifier", IPC_INPUT_GUARD); // upstream 4
      p.addLast("dataHandler", messageHandler); // upstream 6

      return p;
    }
  }

  public static class ClientPipelineFactory implements ChannelPipelineFactory {
    protected final IPCSessionManagerClient ipcSessionManagerClient;

    protected final MessageChannelHandler<TransportMessage> messageHandler;

    /**
     * constructor.
     * */
    public ClientPipelineFactory(final MessageChannelHandler<TransportMessage> queueMessageHandler) {
      ipcSessionManagerClient = new IPCSessionManagerClient();
      messageHandler = queueMessageHandler;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline p = Channels.pipeline();
      p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder()); // upstream 2
      p.addLast("protobufDecoder", PROTOBUF_DECODER); // upstream 3
      p.addLast("inputVerifier", IPC_INPUT_GUARD); // upstream 4
      p.addLast("ipcSessionManager", ipcSessionManagerClient); // upstream 5
      p.addLast("dataHandler", messageHandler); // upstream 6

      // p.addLast("compressionEncoder", new ZlibEncoder(ZlibWrapper.NONE, 1)); // downstream 1
      p.addLast("frameEncoder", FRAME_ENCODER); // downstream 2
      p.addLast("protobufEncoder", PROTOBUF_ENCODER); // downstream 3
      // p.addLast("flowControl", flowController);

      return p;
    }
  }

  public static class ServerPipelineFactory implements ChannelPipelineFactory {

    private final MessageChannelHandler<TransportMessage> messageProcessor;

    protected final IPCSessionManagerServer ipcSessionManagerServer;

    // protected final FlowControlHandler flowController;

    /**
     * constructor.
     * */
    public ServerPipelineFactory(final IPCConnectionPool ipcConnectionPool,
        final MessageChannelHandler<TransportMessage> messageProcessor,
        final OrderedMemoryAwareThreadPoolExecutor executor) {
      this.messageProcessor = messageProcessor;
      ipcSessionManagerServer = new IPCSessionManagerServer(ipcConnectionPool);
      // flowController = new FlowControlHandler(outputChannel2Task);
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline p = Channels.pipeline();
      p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder()); // upstream 2
      p.addLast("protobufDecoder", PROTOBUF_DECODER); // upstream 3
      p.addLast("inputVerifier", IPC_INPUT_GUARD); // upstream 4
      p.addLast("ipcSessionManager", ipcSessionManagerServer); // upstream 5
      p.addLast("dataHandler", messageProcessor); // upstream 6

      p.addLast("frameEncoder", FRAME_ENCODER); // downstream 2
      p.addLast("protobufEncoder", PROTOBUF_ENCODER); // downstream 3
      // p.addLast("flowControl", flowController);

      return p;
    }

  }

  /**
   * protobuf encoder. Protobuf data strucutres -> ChannelBuffer.
   * */
  static final ProtobufEncoder PROTOBUF_ENCODER = new ProtobufEncoder();

  /**
   * separate data streams to data frames.
   * */
  static final ProtobufVarint32LengthFieldPrepender FRAME_ENCODER = new ProtobufVarint32LengthFieldPrepender();

  /**
   * Protobuf decoder. ChannelBuffer -> Protobuf data structures.
   * */
  static final ProtobufDecoder PROTOBUF_DECODER = new ProtobufDecoder(TransportProto.TransportMessage
      .getDefaultInstance());

  static final IPCInputGuard IPC_INPUT_GUARD = new IPCInputGuard();

}

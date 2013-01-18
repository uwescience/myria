package edu.washington.escience.myriad.parallel;

import java.util.concurrent.LinkedBlockingQueue;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

import edu.washington.escience.myriad.parallel.Worker.MessageWrapper;
import edu.washington.escience.myriad.proto.TransportProto;

/**
 * Factories of pipelines.
 * */
public final class IPCPipelineFactories {

  public static class MasterClientPipelineFactory implements ChannelPipelineFactory {

    protected final IPCSessionManagerClient ipcSessionManagerClient;

    protected final MasterDataHandler masterDataHandler;

    /**
     * constructor.
     * */
    MasterClientPipelineFactory(final LinkedBlockingQueue<MessageWrapper> messageQueue,
        final IPCConnectionPool ipcConnectionPool) {
      masterDataHandler = new MasterDataHandler(messageQueue);
      ipcSessionManagerClient = new IPCSessionManagerClient();
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline p = Channels.pipeline();
      // p.addLast("compressionDecoder", new ZlibDecoder(ZlibWrapper.NONE)); // upstream 1
      p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder()); // upstream 2
      p.addLast("protobufDecoder", PROTOBUF_DECODER); // upstream 3
      p.addLast("inputVerifier", IPC_INPUT_GUARD); // upstream 4
      p.addLast("ipcSessionManager", ipcSessionManagerClient); // upstream 5
      p.addLast("dataHandler", masterDataHandler); // upstream 6

      // p.addLast("compressionEncoder", new ZlibEncoder(ZlibWrapper.NONE, 1)); // downstream 1
      p.addLast("frameEncoder", FRAME_ENCODER); // downstream 2
      p.addLast("protobufEncoder", PROTOBUF_ENCODER); // downstream 3

      return p;
    }

  }

  public static class MasterServerPipelineFactory implements ChannelPipelineFactory {

    final protected MasterDataHandler masterDataHandler;

    /**
     * master control handler.
     * */
    protected final IPCSessionManagerServer ipcSessionManagerServer;

    /**
     * constructor.
     * */
    MasterServerPipelineFactory(final LinkedBlockingQueue<MessageWrapper> messageQueue,
        final IPCConnectionPool ipcConnectionPool) {
      masterDataHandler = new MasterDataHandler(messageQueue);
      ipcSessionManagerServer = new IPCSessionManagerServer(ipcConnectionPool);
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline p = Channels.pipeline();
      // p.addLast("compressionDecoder", new ZlibDecoder(ZlibWrapper.NONE)); // upstream 1
      // p.addLast("ioTimestampRecordHandler", IPC_IO_TIMESTAMP_RECORD_HANDLER);
      p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder()); // upstream 2
      p.addLast("protobufDecoder", PROTOBUF_DECODER); // upstream 3
      p.addLast("inputVerifier", IPC_INPUT_GUARD); // upstream 4
      p.addLast("ipcSessionManager", ipcSessionManagerServer); // upstream 5
      p.addLast("dataHandler", masterDataHandler); // upstream 6

      // p.addLast("compressionEncoder", new ZlibEncoder(ZlibWrapper.NONE, 1)); // downstream 1
      p.addLast("frameEncoder", FRAME_ENCODER); // downstream 2
      p.addLast("protobufEncoder", PROTOBUF_ENCODER); // downstream 3

      return p;
    }

  }

  public static class WorkerClientPipelineFactory implements ChannelPipelineFactory {
    // private final LinkedBlockingQueue<MessageWrapper> messageQueue;
    protected final IPCSessionManagerClient ipcSessionManagerClient;

    final WorkerDataHandler workerDataHandler;

    /**
     * constructor.
     * */
    WorkerClientPipelineFactory(final LinkedBlockingQueue<MessageWrapper> messageQueue,
        final IPCConnectionPool ipcConnectionPool) {
      // this.messageQueue = messageQueue;
      ipcSessionManagerClient = new IPCSessionManagerClient();
      workerDataHandler = new WorkerDataHandler(messageQueue);
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline p = Channels.pipeline();
      // p.addLast("compressionDecoder", new ZlibDecoder(ZlibWrapper.NONE)); // upstream 1
      p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder()); // upstream 2
      p.addLast("protobufDecoder", PROTOBUF_DECODER); // upstream 3
      p.addLast("inputVerifier", IPC_INPUT_GUARD); // upstream 4
      p.addLast("ipcSessionManager", ipcSessionManagerClient); // upstream 5
      p.addLast("dataHandler", workerDataHandler); // upstream 6

      // p.addLast("compressionEncoder", new ZlibEncoder(ZlibWrapper.NONE, 1)); // downstream 1
      p.addLast("frameEncoder", FRAME_ENCODER); // downstream 2
      p.addLast("protobufEncoder", PROTOBUF_ENCODER); // downstream 3

      return p;
    }
  }

  public static class WorkerServerPipelineFactory implements ChannelPipelineFactory {

    private final WorkerDataHandler workerDataHandler;

    protected final IPCSessionManagerServer ipcSessionManagerServer;

    /**
     * constructor.
     * */
    WorkerServerPipelineFactory(final LinkedBlockingQueue<MessageWrapper> messageQueue,
        final IPCConnectionPool ipcConnectionPool) {
      workerDataHandler = new WorkerDataHandler(messageQueue);
      ipcSessionManagerServer = new IPCSessionManagerServer(ipcConnectionPool);
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline p = Channels.pipeline();
      // p.addLast("compressionDecoder", new ZlibDecoder(ZlibWrapper.NONE)); // upstream 1
      // p.addLast("ioTimestampRecordHandler", IPC_IO_TIMESTAMP_RECORD_HANDLER);
      p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder()); // upstream 2
      p.addLast("protobufDecoder", PROTOBUF_DECODER); // upstream 3
      p.addLast("inputVerifier", IPC_INPUT_GUARD); // upstream 4
      p.addLast("ipcSessionManager", ipcSessionManagerServer); // upstream 5
      p.addLast("dataHandler", workerDataHandler); // upstream 6

      // p.addLast("compressionEncoder", new ZlibEncoder(ZlibWrapper.NONE, 1)); // downstream 1
      p.addLast("frameEncoder", FRAME_ENCODER); // downstream 2
      p.addLast("protobufEncoder", PROTOBUF_ENCODER); // downstream 3

      return p;
    }

  }

  /**
   * protobuf encoder. Protobuf data strucutres -> ChannelBuffer.
   * */
  protected static final ProtobufEncoder PROTOBUF_ENCODER = new ProtobufEncoder();

  // protected static final IOTimestampRecordHandler IPC_IO_TIMESTAMP_RECORD_HANDLER = new IOTimestampRecordHandler();

  /**
   * separate data streams to data frames.
   * */
  protected static final ProtobufVarint32LengthFieldPrepender FRAME_ENCODER =
      new ProtobufVarint32LengthFieldPrepender();

  /**
   * Protobuf decoder. ChannelBuffer -> Protobuf data structures.
   * */
  protected static final ProtobufDecoder PROTOBUF_DECODER = new ProtobufDecoder(TransportProto.TransportMessage
      .getDefaultInstance());

  /**
   * The first processor of incoming data packages. Filtering out invalid data packages, and do basic error processing.
   * */
  protected static final IPCInputGuard IPC_INPUT_GUARD = new IPCInputGuard();

  private IPCPipelineFactories() {
  }
}

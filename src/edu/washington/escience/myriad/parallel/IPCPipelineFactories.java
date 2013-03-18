package edu.washington.escience.myriad.parallel;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.jboss.netty.handler.execution.ExecutionHandler;

import edu.washington.escience.myriad.parallel.ipc.IPCSessionManagerClient;
import edu.washington.escience.myriad.parallel.ipc.IPCSessionManagerServer;
import edu.washington.escience.myriad.proto.TransportProto;

/**
 * Factories of pipelines.
 * */
public final class IPCPipelineFactories {

  /**
   * In JVM pipeline factory for the master.
   * */
  public static final class MasterInJVMPipelineFactory implements ChannelPipelineFactory {

    private final Server theMaster;

    public MasterInJVMPipelineFactory(final Server theMaster) {
      this.theMaster = theMaster;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline p = Channels.pipeline();
      p.addLast("inputVerifier", IPC_INPUT_GUARD); // upstream 4
      p.addLast("flowControl", theMaster.flowController);
      p.addLast("dataHandler", theMaster.masterDataHandler); // upstream 6
      return p;
    }
  }

  /**
   * In JVM pipeline factory for workers.
   * */
  public static final class WorkerInJVMPipelineFactory implements ChannelPipelineFactory {

    private final Worker ownerWorker;

    public WorkerInJVMPipelineFactory(final Worker ownerWorker) {
      this.ownerWorker = ownerWorker;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline p = Channels.pipeline();
      p.addLast("inputVerifier", IPC_INPUT_GUARD); // upstream 4
      p.addLast("flowControl", ownerWorker.flowController);
      p.addLast("dataHandler", ownerWorker.workerDataHandler); // upstream 6
      return p;
    }
  }

  public static final class MasterClientPipelineFactory implements ChannelPipelineFactory {

    private final IPCSessionManagerClient ipcSessionManagerClient;

    private final ExecutionHandler pipelineExecutionHandler;

    private final Server ownerMaster;

    /**
     * @param theMaster the owner master
     * */
    MasterClientPipelineFactory(final Server theMaster) {
      ownerMaster = theMaster;
      ipcSessionManagerClient = new IPCSessionManagerClient();
      if (theMaster.ipcPipelineExecutor != null) {
        pipelineExecutionHandler = new ExecutionHandler(theMaster.ipcPipelineExecutor);
      } else {
        pipelineExecutionHandler = null;
      }
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline p = Channels.pipeline();
      // p.addLast("compressionDecoder", new ZlibDecoder(ZlibWrapper.NONE)); // upstream 1
      p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder()); // upstream 2
      p.addLast("protobufDecoder", PROTOBUF_DECODER); // upstream 3
      // p.addLast("compressionEncoder", new ZlibEncoder(ZlibWrapper.NONE, 1)); // downstream 1
      p.addLast("frameEncoder", FRAME_ENCODER); // downstream 2
      p.addLast("protobufEncoder", PROTOBUF_ENCODER); // downstream 3

      if (pipelineExecutionHandler != null) {
        p.addLast("executor", pipelineExecutionHandler);
      }
      p.addLast("inputVerifier", IPC_INPUT_GUARD); // upstream 4
      p.addLast("ipcSessionManager", ipcSessionManagerClient); // upstream 5
      p.addLast("flowControl", ownerMaster.flowController);
      p.addLast("dataHandler", ownerMaster.masterDataHandler); // upstream 6
      return p;
    }

  }

  public static final class MasterServerPipelineFactory implements ChannelPipelineFactory {
    /**
     * master control handler.
     * */
    private final IPCSessionManagerServer ipcSessionManagerServer;

    private final ExecutionHandler pipelineExecutionHandler;

    private final Server ownerMaster;

    /**
     * constructor.
     * */
    MasterServerPipelineFactory(final Server theMaster) {
      ownerMaster = theMaster;
      ipcSessionManagerServer = new IPCSessionManagerServer(theMaster.connectionPool);
      if (theMaster.ipcPipelineExecutor != null) {
        pipelineExecutionHandler = new ExecutionHandler(theMaster.ipcPipelineExecutor);
      } else {
        pipelineExecutionHandler = null;
      }
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline p = Channels.pipeline();
      p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder()); // upstream 2
      p.addLast("protobufDecoder", PROTOBUF_DECODER); // upstream 3
      p.addLast("frameEncoder", FRAME_ENCODER); // downstream 2
      p.addLast("protobufEncoder", PROTOBUF_ENCODER); // downstream 3

      if (pipelineExecutionHandler != null) {
        p.addLast("executor", pipelineExecutionHandler);
      }
      p.addLast("inputVerifier", IPC_INPUT_GUARD); // upstream 4
      p.addLast("ipcSessionManager", ipcSessionManagerServer); // upstream 5
      p.addLast("flowControl", ownerMaster.flowController);
      p.addLast("dataHandler", ownerMaster.masterDataHandler); // upstream 6

      return p;
    }
  }

  public static final class WorkerClientPipelineFactory implements ChannelPipelineFactory {

    private final IPCSessionManagerClient ipcSessionManagerClient;

    private final Worker ownerWorker;

    private final ExecutionHandler pipelineExecutionHandler;

    /**
     * constructor.
     * */
    public WorkerClientPipelineFactory(final Worker ownerWorker) {
      this.ownerWorker = ownerWorker;
      ipcSessionManagerClient = new IPCSessionManagerClient();
      if (ownerWorker.pipelineExecutor != null) {
        pipelineExecutionHandler = new ExecutionHandler(ownerWorker.pipelineExecutor);
      } else {
        pipelineExecutionHandler = null;
      }
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline p = Channels.pipeline();
      p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder()); // upstream 2
      p.addLast("protobufDecoder", PROTOBUF_DECODER); // upstream 3
      // p.addLast("compressionEncoder", new ZlibEncoder(ZlibWrapper.NONE, 1)); // downstream 1
      p.addLast("frameEncoder", FRAME_ENCODER); // downstream 2
      p.addLast("protobufEncoder", PROTOBUF_ENCODER); // downstream 3
      if (pipelineExecutionHandler != null) {
        p.addLast("executor", pipelineExecutionHandler);
      }

      p.addLast("inputVerifier", IPC_INPUT_GUARD); // upstream 4
      p.addLast("ipcSessionManager", ipcSessionManagerClient); // upstream 5
      p.addLast("flowControl", ownerWorker.flowController);
      p.addLast("dataHandler", ownerWorker.workerDataHandler); // upstream 6

      return p;
    }
  }

  public static final class WorkerServerPipelineFactory implements ChannelPipelineFactory {

    private final IPCSessionManagerServer ipcSessionManagerServer;

    private final ExecutionHandler pipelineExecutionHandler;

    private final Worker ownerWorker;

    /**
     * constructor.
     * 
     * @param ownerWorker the worker who is the owner of the pipeline factory.
     * */
    public WorkerServerPipelineFactory(final Worker ownerWorker) {
      this.ownerWorker = ownerWorker;
      ipcSessionManagerServer = new IPCSessionManagerServer(ownerWorker.connectionPool);
      if (ownerWorker.pipelineExecutor != null) {
        pipelineExecutionHandler = new ExecutionHandler(ownerWorker.pipelineExecutor);
      } else {
        pipelineExecutionHandler = null;
      }
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline p = Channels.pipeline();
      // p.addLast("compressionDecoder", new ZlibDecoder(ZlibWrapper.NONE)); // upstream 1
      // p.addLast("ioTimestampRecordHandler", IPC_IO_TIMESTAMP_RECORD_HANDLER);
      p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder()); // upstream 2
      p.addLast("protobufDecoder", PROTOBUF_DECODER); // upstream 3
      // p.addLast("compressionEncoder", new ZlibEncoder(ZlibWrapper.NONE, 1)); // downstream 1
      p.addLast("frameEncoder", FRAME_ENCODER); // downstream 2
      p.addLast("protobufEncoder", PROTOBUF_ENCODER); // downstream 3

      if (pipelineExecutionHandler != null) {
        p.addLast("executor", pipelineExecutionHandler);
      }
      p.addLast("inputVerifier", IPC_INPUT_GUARD); // upstream 4
      p.addLast("ipcSessionManager", ipcSessionManagerServer); // upstream 5
      p.addLast("flowControl", ownerWorker.flowController);
      p.addLast("dataHandler", ownerWorker.workerDataHandler); // upstream 6

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

  /**
   * The first processor of incoming data packages. Filtering out invalid data packages, and do basic error processing.
   * */
  static final IPCInputGuard IPC_INPUT_GUARD = new IPCInputGuard();

  /**
   * Utility class.
   * */
  private IPCPipelineFactories() {
  }
}

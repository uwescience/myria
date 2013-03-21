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

    /**
     * The master who will be the owner of the pipeline factory.
     * */
    private final Server theMaster;

    /**
     * @param theMaster the master who will be the owner of the pipeline factory.
     * */
    public MasterInJVMPipelineFactory(final Server theMaster) {
      this.theMaster = theMaster;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline p = Channels.pipeline();
      p.addLast("inputVerifier", IPC_INPUT_GUARD); // upstream 4
      p.addLast("flowControl", theMaster.getFlowControlHandler());
      p.addLast("dataHandler", theMaster.getDataHandler()); // upstream 6
      return p;
    }
  }

  /**
   * In JVM pipeline factory for workers.
   * */
  public static final class WorkerInJVMPipelineFactory implements ChannelPipelineFactory {

    /**
     * The worker who will be the owner of the pipeline factory.
     * */
    private final Worker ownerWorker;

    /**
     * @param ownerWorker the worker who will be the owner of the pipeline factory.
     * */
    public WorkerInJVMPipelineFactory(final Worker ownerWorker) {
      this.ownerWorker = ownerWorker;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline p = Channels.pipeline();
      p.addLast("inputVerifier", IPC_INPUT_GUARD); // upstream 4
      p.addLast("flowControl", ownerWorker.getFlowControlHandler());
      p.addLast("dataHandler", ownerWorker.getWorkerDataHandler()); // upstream 6
      return p;
    }
  }

  /**
   * Client side pipeline factory for the master.
   * */
  public static final class MasterClientPipelineFactory implements ChannelPipelineFactory {
    /**
     * IPC session management.
     * */
    private final IPCSessionManagerClient ipcSessionManagerClient;

    /**
     * pipe line executor. A dedicated executor service who executes the handlers in a the pipeline.
     * */
    private final ExecutionHandler pipelineExecutionHandler;

    /**
     * The master who will be the owner of the pipeline factory.
     * */
    private final Server ownerMaster;

    /**
     * @param theMaster the owner master
     * */
    MasterClientPipelineFactory(final Server theMaster) {
      ownerMaster = theMaster;
      ipcSessionManagerClient = new IPCSessionManagerClient();
      if (theMaster.getPipelineExecutor() != null) {
        pipelineExecutionHandler = new ExecutionHandler(theMaster.getPipelineExecutor());
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
      p.addLast("flowControl", ownerMaster.getFlowControlHandler());
      p.addLast("dataHandler", ownerMaster.getDataHandler()); // upstream 6
      return p;
    }

  }
  /**
   * Server side pipeline factory for the master.
   * */
  public static final class MasterServerPipelineFactory implements ChannelPipelineFactory {
    /**
     * master control handler.
     * */
    private final IPCSessionManagerServer ipcSessionManagerServer;

    /**
     * pipe line executor.
     * */
    private final ExecutionHandler pipelineExecutionHandler;

    /**
     * The master who will be the owner of the pipeline factory.
     * */
    private final Server ownerMaster;

    /**
     * constructor.
     * 
     * @param theMaster the master who will be the owner of the pipeline factory.
     * */
    MasterServerPipelineFactory(final Server theMaster) {
      ownerMaster = theMaster;
      ipcSessionManagerServer = new IPCSessionManagerServer(theMaster.getIPCConnectionPool());
      if (theMaster.getPipelineExecutor() != null) {
        pipelineExecutionHandler = new ExecutionHandler(theMaster.getPipelineExecutor());
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
      p.addLast("flowControl", ownerMaster.getFlowControlHandler());
      p.addLast("dataHandler", ownerMaster.getDataHandler()); // upstream 6

      return p;
    }
  }

  /**
   * Client side pipeline factory for workers.
   * */
  public static final class WorkerClientPipelineFactory implements ChannelPipelineFactory {
    /**
     * IPC session management.
     * */
    private final IPCSessionManagerClient ipcSessionManagerClient;

    /**
     * The worker who will be the owner of the pipeline factory.
     * */
    private final Worker ownerWorker;

    /**
     * pipe line executor.
     * */
    private final ExecutionHandler pipelineExecutionHandler;

    /**
     * constructor.
     * 
     * @param ownerWorker the worker who will be the owner of the pipeline factory.
     * */
    public WorkerClientPipelineFactory(final Worker ownerWorker) {
      this.ownerWorker = ownerWorker;
      ipcSessionManagerClient = new IPCSessionManagerClient();
      if (ownerWorker.getPipelineExecutor() != null) {
        pipelineExecutionHandler = new ExecutionHandler(ownerWorker.getPipelineExecutor());
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
      p.addLast("flowControl", ownerWorker.getFlowControlHandler());
      p.addLast("dataHandler", ownerWorker.getWorkerDataHandler()); // upstream 6

      return p;
    }
  }

  /**
   * Server side pipeline factory for workers.
   * */
  public static final class WorkerServerPipelineFactory implements ChannelPipelineFactory {

    /**
     * IPC session management.
     * */
    private final IPCSessionManagerServer ipcSessionManagerServer;

    /**
     * pipe line executor.
     * */
    private final ExecutionHandler pipelineExecutionHandler;

    /**
     * The worker who will be the owner of the pipeline factory.
     * */
    private final Worker ownerWorker;

    /**
     * constructor.
     * 
     * @param ownerWorker the worker who is the owner of the pipeline factory.
     * */
    public WorkerServerPipelineFactory(final Worker ownerWorker) {
      this.ownerWorker = ownerWorker;
      ipcSessionManagerServer = new IPCSessionManagerServer(ownerWorker.getIPCConnectionPool());
      if (ownerWorker.getPipelineExecutor() != null) {
        pipelineExecutionHandler = new ExecutionHandler(ownerWorker.getPipelineExecutor());
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
      p.addLast("flowControl", ownerWorker.getFlowControlHandler());
      p.addLast("dataHandler", ownerWorker.getWorkerDataHandler()); // upstream 6

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

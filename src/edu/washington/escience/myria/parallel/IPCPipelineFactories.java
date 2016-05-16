package edu.washington.escience.myria.parallel;

import java.util.concurrent.ExecutorService;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.jboss.netty.handler.execution.ExecutionHandler;

import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myria.parallel.ipc.IPCMessageHandler;

/**
 * Factories of pipelines.
 * */
public final class IPCPipelineFactories {

  /**
   * In JVM pipeline factory for the master.
   * */
  public static class MasterInJVMPipelineFactory implements ChannelPipelineFactory {

    /**
     * IPC session management.
     * */
    private final IPCMessageHandler ipcMessageHandler;

    /**
     * @param pool the ipc connection pool.
     * */
    public MasterInJVMPipelineFactory(final IPCConnectionPool pool) {
      ipcMessageHandler = new IPCMessageHandler(pool);
    }

    @Override
    public final ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline p = Channels.pipeline();
      p.addLast("ipcMessageHandler", ipcMessageHandler); // upstream 5
      return p;
    }
  }

  /**
   * In JVM pipeline factory for workers.
   * */
  public static final class WorkerInJVMPipelineFactory extends MasterInJVMPipelineFactory {

    /**
     * @param pool the ipc connection pool.
     * */
    public WorkerInJVMPipelineFactory(final IPCConnectionPool pool) {
      super(pool);
    }
  }

  /**
   * Client side pipeline factory for the master.
   * */
  public static class MasterClientPipelineFactory implements ChannelPipelineFactory {
    /**
     * IPC session management.
     * */
    private final IPCMessageHandler ipcMessageHandler;

    /**
     * pipe line executor. A dedicated executor service who executes the handlers in a the pipeline.
     * */
    private final ExecutionHandler pipelineExecutionHandler;

    /**
     * @param pool the owner IPCConnectionPool
     * @param pipelineExecutor possible pipeline executor, null is allowed.
     * */
    MasterClientPipelineFactory(
        final IPCConnectionPool pool, final ExecutorService pipelineExecutor) {
      ipcMessageHandler = new IPCMessageHandler(pool);
      if (pipelineExecutor != null) {
        pipelineExecutionHandler = new ExecutionHandler(pipelineExecutor);
      } else {
        pipelineExecutionHandler = null;
      }
    }

    @Override
    public final ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline p = Channels.pipeline();
      p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder()); // upstream 2
      p.addLast("frameEncoder", FRAME_ENCODER); // downstream 2

      if (pipelineExecutionHandler != null) {
        p.addLast("executor", pipelineExecutionHandler);
      }
      p.addLast("ipcMessageHandler", ipcMessageHandler); // upstream 5
      return p;
    }
  }
  /**
   * Server side pipeline factory for the master.
   * */
  public static final class MasterServerPipelineFactory extends MasterClientPipelineFactory {

    /**
     * @param pool the owner IPCConnectionPool
     * @param pipelineExecutor possible pipeline executor, null is allowed.
     * */
    MasterServerPipelineFactory(
        final IPCConnectionPool pool, final ExecutorService pipelineExecutor) {
      super(pool, pipelineExecutor);
    }
  }

  /**
   * Server side pipeline factory for the master.
   * */
  public static final class WorkerClientPipelineFactory extends MasterClientPipelineFactory {

    /**
     * @param pool the owner IPCConnectionPool
     * @param pipelineExecutor possible pipeline executor, null is allowed.
     * */
    WorkerClientPipelineFactory(
        final IPCConnectionPool pool, final ExecutorService pipelineExecutor) {
      super(pool, pipelineExecutor);
    }
  }

  /**
   * Client side pipeline factory for workers.
   * */
  public static final class WorkerServerPipelineFactory extends MasterClientPipelineFactory {

    /**
     * @param pool the owner IPCConnectionPool
     * @param pipelineExecutor possible pipeline executor, null is allowed.
     * */
    WorkerServerPipelineFactory(
        final IPCConnectionPool pool, final ExecutorService pipelineExecutor) {
      super(pool, pipelineExecutor);
    }
  }

  /**
   * separate data streams to data frames.
   * */
  static final ProtobufVarint32LengthFieldPrepender FRAME_ENCODER =
      new ProtobufVarint32LengthFieldPrepender();

  /**
   * Utility class.
   * */
  private IPCPipelineFactories() {}
}

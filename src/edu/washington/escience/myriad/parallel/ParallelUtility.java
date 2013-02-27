package edu.washington.escience.myriad.parallel;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import edu.washington.escience.myriad.parallel.Worker.MessageWrapper;

/**
 * Utility methods.
 */
public final class ParallelUtility {

  /** Constant. */
  private static final int ONE_KB_IN_BYTES = 1024;
  /** Constant parameter. */
  private static final long SEND_BUFFER_SIZE_BYTES = 512 * ONE_KB_IN_BYTES;
  /** Constant parameter. */
  private static final long RECEIVE_BUFFER_SIZE_BYTES = 512 * ONE_KB_IN_BYTES;
  /** Constant parameter. */
  private static final long WRITE_BUFFER_LOW_MARK_BYTES = 16 * ONE_KB_IN_BYTES;
  /** Constant parameter. */
  private static final long WRITE_BUFFER_HIGH_MARK_BYTES = 256 * ONE_KB_IN_BYTES;
  /** Constant parameter. */
  private static final long CONNECT_TIMEOUT_MILLIS = 3 * 1000;

  /**
   * Create a client side connector to the server.
   * 
   * @param clientFactory All the client connections share the same generation factory, basically, they share the same
   *          thread pool
   */
  static ClientBootstrap createIPCClient(final ChannelFactory clientFactory, final ChannelPipelineFactory cpf) {

    // Create the bootstrap
    final ClientBootstrap bootstrap = new ClientBootstrap(clientFactory);
    bootstrap.setPipelineFactory(cpf);
    bootstrap.setOption("tcpNoDelay", true);
    bootstrap.setOption("keepAlive", false);
    bootstrap.setOption("reuseAddress", true);
    bootstrap.setOption("connectTimeoutMillis", CONNECT_TIMEOUT_MILLIS);
    bootstrap.setOption("sendBufferSize", SEND_BUFFER_SIZE_BYTES);
    bootstrap.setOption("receiveBufferSize", RECEIVE_BUFFER_SIZE_BYTES);
    bootstrap.setOption("writeBufferLowWaterMark", WRITE_BUFFER_LOW_MARK_BYTES);
    bootstrap.setOption("writeBufferHighWaterMark", WRITE_BUFFER_HIGH_MARK_BYTES);

    return bootstrap;
  }

  /**
   * Create a server side acceptor.
   */
  public static ServerBootstrap createMasterIPCServer(final LinkedBlockingQueue<MessageWrapper> messageQueue,
      final IPCConnectionPool ipcConnectionPool) {

    // Start server with Nb of active threads = 2*NB CPU + 1 as maximum.
    final ChannelFactory factory =
        new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool(), Runtime
            .getRuntime().availableProcessors() * 2 + 1);

    final ServerBootstrap bootstrap = new ServerBootstrap(factory);

    bootstrap.setPipelineFactory(new IPCPipelineFactories.MasterServerPipelineFactory(messageQueue, ipcConnectionPool));

    bootstrap.setOption("child.tcpNoDelay", true);
    bootstrap.setOption("child.keepAlive", false);
    bootstrap.setOption("child.reuseAddress", true);
    bootstrap.setOption("child.connectTimeoutMillis", CONNECT_TIMEOUT_MILLIS);
    bootstrap.setOption("child.sendBufferSize", SEND_BUFFER_SIZE_BYTES);
    bootstrap.setOption("child.receiveBufferSize", RECEIVE_BUFFER_SIZE_BYTES);
    bootstrap.setOption("child.writeBufferLowWaterMark", WRITE_BUFFER_LOW_MARK_BYTES);
    bootstrap.setOption("child.writeBufferHighWaterMark", WRITE_BUFFER_HIGH_MARK_BYTES);

    bootstrap.setOption("readWriteFair", true);

    return bootstrap;
  }

  /**
   * Create a server side acceptor.
   * 
   * @param messageQueue
   * @param ipcConnectionPool
   * @return
   */
  public static ServerBootstrap createWorkerIPCServer(final LinkedBlockingQueue<MessageWrapper> messageQueue,
      final IPCConnectionPool ipcConnectionPool) {

    // Start server with Nb of active threads = 2*NB CPU + 1 as maximum.
    final ChannelFactory factory =
        new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool(), Runtime
            .getRuntime().availableProcessors() * 2 + 1);

    final ServerBootstrap bootstrap = new ServerBootstrap(factory);

    bootstrap.setPipelineFactory(new IPCPipelineFactories.WorkerServerPipelineFactory(messageQueue, ipcConnectionPool));

    bootstrap.setOption("child.tcpNoDelay", true);
    bootstrap.setOption("child.keepAlive", false);
    bootstrap.setOption("child.reuseAddress", true);
    bootstrap.setOption("child.connectTimeoutMillis", CONNECT_TIMEOUT_MILLIS);
    bootstrap.setOption("child.sendBufferSize", SEND_BUFFER_SIZE_BYTES);
    bootstrap.setOption("child.receiveBufferSize", RECEIVE_BUFFER_SIZE_BYTES);
    bootstrap.setOption("child.writeBufferLowWaterMark", WRITE_BUFFER_LOW_MARK_BYTES);
    bootstrap.setOption("child.writeBufferHighWaterMark", WRITE_BUFFER_HIGH_MARK_BYTES);

    bootstrap.setOption("readWriteFair", true);

    return bootstrap;
  }

  public static String[] removeArg(final String[] args, final int toBeRemoved) {
    if (args == null) {
      return null;
    }

    if (toBeRemoved < 0 || toBeRemoved >= args.length) {
      return args;
    }
    final String[] newArgs = new String[args.length - 1];
    System.arraycopy(args, 0, newArgs, 0, toBeRemoved);
    System.arraycopy(args, toBeRemoved + 1, newArgs, toBeRemoved, args.length - toBeRemoved - 1);
    return newArgs;
  }

  /** Prevent construction of utility class. */
  private ParallelUtility() {
  }

}

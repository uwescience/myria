package edu.washington.escience.myria.parallel;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;

import edu.washington.escience.myria.coordinator.ConfigFileException;

/**
 * The configurations of the IPC layer.
 */
public final class IPCConfigurations {

  /**
   * @return a pre-configured IPC client connection bootstrap.
   * @param theMaster the master
   * @throws ConfigFileException
   */
  public static ClientBootstrap createMasterIPCClientBootstrap(final int connectTimeoutMillis,
      final int sendBufferSize, final int receiveBufferSize, final int writeBufferLowWaterMark,
      final int writeBufferHighWaterMark) {

    final ClientBootstrap bootstrap = new ClientBootstrap();

    bootstrap.setOption("tcpNoDelay", true);
    bootstrap.setOption("keepAlive", false);
    bootstrap.setOption("reuseAddress", true);

    bootstrap.setOption("connectTimeoutMillis", connectTimeoutMillis);
    bootstrap.setOption("sendBufferSize", sendBufferSize);
    bootstrap.setOption("receiveBufferSize", receiveBufferSize);
    bootstrap.setOption("writeBufferLowWaterMark", writeBufferLowWaterMark);
    bootstrap.setOption("writeBufferHighWaterMark", writeBufferHighWaterMark);

    return bootstrap;
  }

  /**
   * @return a pre-configured IPC client connection bootstrap.
   * @param worker the owner worker
   * @throws ConfigFileException
   */
  public static ClientBootstrap createWorkerIPCClientBootstrap(final int connectTimeoutMillis,
      final int sendBufferSize, final int receiveBufferSize, final int writeBufferLowWaterMark,
      final int writeBufferHighWaterMark) {

    final ClientBootstrap bootstrap = new ClientBootstrap();

    bootstrap.setOption("tcpNoDelay", true);
    bootstrap.setOption("keepAlive", false);
    bootstrap.setOption("reuseAddress", true);

    bootstrap.setOption("connectTimeoutMillis", connectTimeoutMillis);
    bootstrap.setOption("sendBufferSize", sendBufferSize);
    bootstrap.setOption("receiveBufferSize", receiveBufferSize);
    bootstrap.setOption("writeBufferLowWaterMark", writeBufferLowWaterMark);
    bootstrap.setOption("writeBufferHighWaterMark", writeBufferHighWaterMark);

    return bootstrap;
  }

  /**
   * @return a pre-configured IPC server bootstrap for Master.
   * @param theMaster the master
   * @throws ConfigFileException
   * @throws NumberFormatException
   */
  public static ServerBootstrap createMasterIPCServerBootstrap(final int connectTimeoutMillis,
      final int sendBufferSize, final int receiveBufferSize, final int writeBufferLowWaterMark,
      final int writeBufferHighWaterMark) {

    final ServerBootstrap bootstrap = new ServerBootstrap();

    bootstrap.setOption("reuseAddress", true);
    bootstrap.setOption("child.tcpNoDelay", true);
    bootstrap.setOption("child.keepAlive", false);
    bootstrap.setOption("child.reuseAddress", true);
    bootstrap.setOption("readWriteFair", true);

    bootstrap.setOption("child.connectTimeoutMillis", connectTimeoutMillis);
    bootstrap.setOption("child.sendBufferSize", sendBufferSize);
    bootstrap.setOption("child.receiveBufferSize", receiveBufferSize);
    bootstrap.setOption("child.writeBufferLowWaterMark", writeBufferLowWaterMark);
    bootstrap.setOption("child.writeBufferHighWaterMark", writeBufferHighWaterMark);

    return bootstrap;
  }

  /**
   * @return A pre-configured IPC server bootstrap for Workers.
   * @param worker the owner worker
   * @throws ConfigFileException
   * @throws NumberFormatException
   */
  public static ServerBootstrap createWorkerIPCServerBootstrap(final int connectTimeoutMillis,
      final int sendBufferSize, final int receiveBufferSize, final int writeBufferLowWaterMark,
      final int writeBufferHighWaterMark) {

    final ServerBootstrap bootstrap = new ServerBootstrap();

    bootstrap.setOption("reuseAddress", true);
    bootstrap.setOption("child.tcpNoDelay", true);
    bootstrap.setOption("child.keepAlive", false);
    bootstrap.setOption("child.reuseAddress", true);
    bootstrap.setOption("readWriteFair", true);

    bootstrap.setOption("child.connectTimeoutMillis", connectTimeoutMillis);
    bootstrap.setOption("child.sendBufferSize", sendBufferSize);
    bootstrap.setOption("child.receiveBufferSize", receiveBufferSize);
    bootstrap.setOption("child.writeBufferLowWaterMark", writeBufferLowWaterMark);
    bootstrap.setOption("child.writeBufferHighWaterMark", writeBufferHighWaterMark);

    return bootstrap;
  }

  /** Prevent construction of utility class. */
  private IPCConfigurations() {}

}

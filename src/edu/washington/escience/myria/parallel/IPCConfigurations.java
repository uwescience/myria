package edu.washington.escience.myria.parallel;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;

import edu.washington.escience.myria.MyriaSystemConfigKeys;

/**
 * The configurations of the IPC layer.
 */
public final class IPCConfigurations {

  /**
   * @return a pre-configured IPC client connection bootstrap.
   * @param theMaster the master
   */
  public static ClientBootstrap createMasterIPCClientBootstrap(final Server theMaster) {

    // Create the bootstrap
    final ClientBootstrap bootstrap = new ClientBootstrap();
    bootstrap.setOption("tcpNoDelay", true);
    bootstrap.setOption("keepAlive", false);
    bootstrap.setOption("reuseAddress", true);

    bootstrap.setOption("connectTimeoutMillis", Long.valueOf(theMaster
        .getConfiguration(MyriaSystemConfigKeys.TCP_CONNECTION_TIMEOUT_MILLIS)));
    bootstrap.setOption("sendBufferSize", Long.valueOf(theMaster
        .getConfiguration(MyriaSystemConfigKeys.TCP_SEND_BUFFER_SIZE_BYTES)));
    bootstrap.setOption("receiveBufferSize", Long.valueOf(theMaster
        .getConfiguration(MyriaSystemConfigKeys.TCP_RECEIVE_BUFFER_SIZE_BYTES)));
    bootstrap.setOption("writeBufferLowWaterMark", Long.valueOf(theMaster
        .getConfiguration(MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES)));
    bootstrap.setOption("writeBufferHighWaterMark", Long.valueOf(theMaster
        .getConfiguration(MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES)));

    return bootstrap;
  }

  /**
   * @return a pre-configured IPC client connection bootstrap.
   * @param worker the owner worker
   */
  public static ClientBootstrap createWorkerIPCClientBootstrap(final Worker worker) {

    // Create the bootstrap
    final ClientBootstrap bootstrap = new ClientBootstrap();
    bootstrap.setOption("tcpNoDelay", true);
    bootstrap.setOption("keepAlive", false);
    bootstrap.setOption("reuseAddress", true);

    bootstrap.setOption("connectTimeoutMillis", Long.valueOf(worker
        .getConfiguration(MyriaSystemConfigKeys.TCP_CONNECTION_TIMEOUT_MILLIS)));
    bootstrap.setOption("sendBufferSize", Long.valueOf(worker
        .getConfiguration(MyriaSystemConfigKeys.TCP_SEND_BUFFER_SIZE_BYTES)));
    bootstrap.setOption("receiveBufferSize", Long.valueOf(worker
        .getConfiguration(MyriaSystemConfigKeys.TCP_RECEIVE_BUFFER_SIZE_BYTES)));
    bootstrap.setOption("writeBufferLowWaterMark", Long.valueOf(worker
        .getConfiguration(MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES)));
    bootstrap.setOption("writeBufferHighWaterMark", Long.valueOf(worker
        .getConfiguration(MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES)));

    return bootstrap;
  }

  /**
   * @return a pre-configured IPC server bootstrap for Master.
   * @param theMaster the master
   */
  public static ServerBootstrap createMasterIPCServerBootstrap(final Server theMaster) {

    final ServerBootstrap bootstrap = new ServerBootstrap();

    bootstrap.setOption("reuseAddress", true);
    bootstrap.setOption("child.tcpNoDelay", true);
    bootstrap.setOption("child.keepAlive", false);
    bootstrap.setOption("child.reuseAddress", true);
    bootstrap.setOption("child.connectTimeoutMillis", Long.valueOf(theMaster
        .getConfiguration(MyriaSystemConfigKeys.TCP_CONNECTION_TIMEOUT_MILLIS)));
    bootstrap.setOption("child.sendBufferSize", Long.valueOf(theMaster
        .getConfiguration(MyriaSystemConfigKeys.TCP_SEND_BUFFER_SIZE_BYTES)));
    bootstrap.setOption("child.receiveBufferSize", Long.valueOf(theMaster
        .getConfiguration(MyriaSystemConfigKeys.TCP_RECEIVE_BUFFER_SIZE_BYTES)));
    bootstrap.setOption("child.writeBufferLowWaterMark", Long.valueOf(theMaster
        .getConfiguration(MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES)));
    bootstrap.setOption("child.writeBufferHighWaterMark", Long.valueOf(theMaster
        .getConfiguration(MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES)));
    bootstrap.setOption("readWriteFair", true);

    return bootstrap;
  }

  /**
   * @return A pre-configured IPC server bootstrap for Workers.
   * @param worker the owner worker
   */
  public static ServerBootstrap createWorkerIPCServerBootstrap(final Worker worker) {
    final ServerBootstrap bootstrap = new ServerBootstrap();

    bootstrap.setOption("reuseAddress", true);
    bootstrap.setOption("child.tcpNoDelay", true);
    bootstrap.setOption("child.keepAlive", false);
    bootstrap.setOption("child.reuseAddress", true);
    bootstrap.setOption("child.connectTimeoutMillis", Long.valueOf(worker
        .getConfiguration(MyriaSystemConfigKeys.TCP_CONNECTION_TIMEOUT_MILLIS)));
    bootstrap.setOption("child.sendBufferSize", Long.valueOf(worker
        .getConfiguration(MyriaSystemConfigKeys.TCP_SEND_BUFFER_SIZE_BYTES)));
    bootstrap.setOption("child.receiveBufferSize", Long.valueOf(worker
        .getConfiguration(MyriaSystemConfigKeys.TCP_RECEIVE_BUFFER_SIZE_BYTES)));
    bootstrap.setOption("child.writeBufferLowWaterMark", Long.valueOf(worker
        .getConfiguration(MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES)));
    bootstrap.setOption("child.writeBufferHighWaterMark", Long.valueOf(worker
        .getConfiguration(MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES)));

    bootstrap.setOption("readWriteFair", true);

    return bootstrap;
  }

  /** Prevent construction of utility class. */
  private IPCConfigurations() {
  }

}

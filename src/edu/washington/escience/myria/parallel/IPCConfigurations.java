package edu.washington.escience.myria.parallel;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;

import edu.washington.escience.myria.MyriaSystemConfigKeys;
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
  public static ClientBootstrap createMasterIPCClientBootstrap(final Server theMaster)
      throws ConfigFileException {

    // Create the bootstrap
    final ClientBootstrap bootstrap = new ClientBootstrap();
    bootstrap.setOption("tcpNoDelay", true);
    bootstrap.setOption("keepAlive", false);
    bootstrap.setOption("reuseAddress", true);

    bootstrap.setOption(
        "connectTimeoutMillis",
        Long.valueOf(
            theMaster.getRuntimeConfiguration(
                MyriaSystemConfigKeys.TCP_CONNECTION_TIMEOUT_MILLIS)));
    bootstrap.setOption(
        "sendBufferSize",
        Long.valueOf(
            theMaster.getRuntimeConfiguration(MyriaSystemConfigKeys.TCP_SEND_BUFFER_SIZE_BYTES)));
    bootstrap.setOption(
        "receiveBufferSize",
        Long.valueOf(
            theMaster.getRuntimeConfiguration(
                MyriaSystemConfigKeys.TCP_RECEIVE_BUFFER_SIZE_BYTES)));
    bootstrap.setOption(
        "writeBufferLowWaterMark",
        Long.valueOf(
            theMaster.getRuntimeConfiguration(
                MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES)));
    bootstrap.setOption(
        "writeBufferHighWaterMark",
        Long.valueOf(
            theMaster.getRuntimeConfiguration(
                MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES)));

    return bootstrap;
  }

  /**
   * @return a pre-configured IPC client connection bootstrap.
   * @param worker the owner worker
   * @throws ConfigFileException
   */
  public static ClientBootstrap createWorkerIPCClientBootstrap(final Worker worker)
      throws ConfigFileException {

    // Create the bootstrap
    final ClientBootstrap bootstrap = new ClientBootstrap();
    bootstrap.setOption("tcpNoDelay", true);
    bootstrap.setOption("keepAlive", false);
    bootstrap.setOption("reuseAddress", true);

    bootstrap.setOption(
        "connectTimeoutMillis",
        Long.valueOf(
            worker.getRuntimeConfiguration(MyriaSystemConfigKeys.TCP_CONNECTION_TIMEOUT_MILLIS)));
    bootstrap.setOption(
        "sendBufferSize",
        Long.valueOf(
            worker.getRuntimeConfiguration(MyriaSystemConfigKeys.TCP_SEND_BUFFER_SIZE_BYTES)));
    bootstrap.setOption(
        "receiveBufferSize",
        Long.valueOf(
            worker.getRuntimeConfiguration(MyriaSystemConfigKeys.TCP_RECEIVE_BUFFER_SIZE_BYTES)));
    bootstrap.setOption(
        "writeBufferLowWaterMark",
        Long.valueOf(
            worker.getRuntimeConfiguration(
                MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES)));
    bootstrap.setOption(
        "writeBufferHighWaterMark",
        Long.valueOf(
            worker.getRuntimeConfiguration(
                MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES)));

    return bootstrap;
  }

  /**
   * @return a pre-configured IPC server bootstrap for Master.
   * @param theMaster the master
   * @throws ConfigFileException
   * @throws NumberFormatException
   */
  public static ServerBootstrap createMasterIPCServerBootstrap(final Server theMaster)
      throws ConfigFileException {

    final ServerBootstrap bootstrap = new ServerBootstrap();

    bootstrap.setOption("reuseAddress", true);
    bootstrap.setOption("child.tcpNoDelay", true);
    bootstrap.setOption("child.keepAlive", false);
    bootstrap.setOption("child.reuseAddress", true);
    bootstrap.setOption(
        "child.connectTimeoutMillis",
        Long.valueOf(
            theMaster.getRuntimeConfiguration(
                MyriaSystemConfigKeys.TCP_CONNECTION_TIMEOUT_MILLIS)));
    bootstrap.setOption(
        "child.sendBufferSize",
        Long.valueOf(
            theMaster.getRuntimeConfiguration(MyriaSystemConfigKeys.TCP_SEND_BUFFER_SIZE_BYTES)));
    bootstrap.setOption(
        "child.receiveBufferSize",
        Long.valueOf(
            theMaster.getRuntimeConfiguration(
                MyriaSystemConfigKeys.TCP_RECEIVE_BUFFER_SIZE_BYTES)));
    bootstrap.setOption(
        "child.writeBufferLowWaterMark",
        Long.valueOf(
            theMaster.getRuntimeConfiguration(
                MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES)));
    bootstrap.setOption(
        "child.writeBufferHighWaterMark",
        Long.valueOf(
            theMaster.getRuntimeConfiguration(
                MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES)));
    bootstrap.setOption("readWriteFair", true);

    return bootstrap;
  }

  /**
   * @return A pre-configured IPC server bootstrap for Workers.
   * @param worker the owner worker
   * @throws ConfigFileException
   * @throws NumberFormatException
   */
  public static ServerBootstrap createWorkerIPCServerBootstrap(final Worker worker)
      throws ConfigFileException {
    final ServerBootstrap bootstrap = new ServerBootstrap();

    bootstrap.setOption("reuseAddress", true);
    bootstrap.setOption("child.tcpNoDelay", true);
    bootstrap.setOption("child.keepAlive", false);
    bootstrap.setOption("child.reuseAddress", true);
    bootstrap.setOption(
        "child.connectTimeoutMillis",
        Long.valueOf(
            worker.getRuntimeConfiguration(MyriaSystemConfigKeys.TCP_CONNECTION_TIMEOUT_MILLIS)));
    bootstrap.setOption(
        "child.sendBufferSize",
        Long.valueOf(
            worker.getRuntimeConfiguration(MyriaSystemConfigKeys.TCP_SEND_BUFFER_SIZE_BYTES)));
    bootstrap.setOption(
        "child.receiveBufferSize",
        Long.valueOf(
            worker.getRuntimeConfiguration(MyriaSystemConfigKeys.TCP_RECEIVE_BUFFER_SIZE_BYTES)));
    bootstrap.setOption(
        "child.writeBufferLowWaterMark",
        Long.valueOf(
            worker.getRuntimeConfiguration(
                MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES)));
    bootstrap.setOption(
        "child.writeBufferHighWaterMark",
        Long.valueOf(
            worker.getRuntimeConfiguration(
                MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES)));

    bootstrap.setOption("readWriteFair", true);

    return bootstrap;
  }

  /** Prevent construction of utility class. */
  private IPCConfigurations() {}
}

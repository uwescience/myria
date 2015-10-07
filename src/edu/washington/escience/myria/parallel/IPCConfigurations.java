package edu.washington.escience.myria.parallel;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.exceptions.InjectionException;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;

import edu.washington.escience.myria.coordinator.ConfigFileException;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule;

/**
 * The configurations of the IPC layer.
 */
public final class IPCConfigurations {

  /**
   * @return a pre-configured IPC client connection bootstrap.
   * @param theMaster the master
   * @throws ConfigFileException
   */
  public static ClientBootstrap createMasterIPCClientBootstrap(final Injector injector)
      throws InjectionException {

    // Create the bootstrap
    final ClientBootstrap bootstrap = new ClientBootstrap();
    bootstrap.setOption("tcpNoDelay", true);
    bootstrap.setOption("keepAlive", false);
    bootstrap.setOption("reuseAddress", true);

    final int connectTimeoutMillis =
        injector.getNamedInstance(MyriaGlobalConfigurationModule.TcpConnectionTimeoutMillis.class);
    final int sendBufferSize =
        injector.getNamedInstance(MyriaGlobalConfigurationModule.TcpSendBufferSizeBytes.class);
    final int receiveBufferSize =
        injector.getNamedInstance(MyriaGlobalConfigurationModule.TcpReceiveBufferSizeBytes.class);
    final int writeBufferLowWaterMark =
        injector
            .getNamedInstance(MyriaGlobalConfigurationModule.FlowControlWriteBufferLowMarkBytes.class);
    final int writeBufferHighWaterMark =
        injector
            .getNamedInstance(MyriaGlobalConfigurationModule.FlowControlWriteBufferHighMarkBytes.class);

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
  public static ClientBootstrap createWorkerIPCClientBootstrap(final Injector injector)
      throws InjectionException {

    // Create the bootstrap
    final ClientBootstrap bootstrap = new ClientBootstrap();
    bootstrap.setOption("tcpNoDelay", true);
    bootstrap.setOption("keepAlive", false);
    bootstrap.setOption("reuseAddress", true);

    final int connectTimeoutMillis =
        injector.getNamedInstance(MyriaGlobalConfigurationModule.TcpConnectionTimeoutMillis.class);
    final int sendBufferSize =
        injector.getNamedInstance(MyriaGlobalConfigurationModule.TcpSendBufferSizeBytes.class);
    final int receiveBufferSize =
        injector.getNamedInstance(MyriaGlobalConfigurationModule.TcpReceiveBufferSizeBytes.class);
    final int writeBufferLowWaterMark =
        injector
            .getNamedInstance(MyriaGlobalConfigurationModule.FlowControlWriteBufferLowMarkBytes.class);
    final int writeBufferHighWaterMark =
        injector
            .getNamedInstance(MyriaGlobalConfigurationModule.FlowControlWriteBufferHighMarkBytes.class);

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
  public static ServerBootstrap createMasterIPCServerBootstrap(final Injector injector)
      throws InjectionException {

    final ServerBootstrap bootstrap = new ServerBootstrap();

    bootstrap.setOption("reuseAddress", true);
    bootstrap.setOption("child.tcpNoDelay", true);
    bootstrap.setOption("child.keepAlive", false);
    bootstrap.setOption("child.reuseAddress", true);

    final int connectTimeoutMillis =
        injector.getNamedInstance(MyriaGlobalConfigurationModule.TcpConnectionTimeoutMillis.class);
    final int sendBufferSize =
        injector.getNamedInstance(MyriaGlobalConfigurationModule.TcpSendBufferSizeBytes.class);
    final int receiveBufferSize =
        injector.getNamedInstance(MyriaGlobalConfigurationModule.TcpReceiveBufferSizeBytes.class);
    final int writeBufferLowWaterMark =
        injector
            .getNamedInstance(MyriaGlobalConfigurationModule.FlowControlWriteBufferLowMarkBytes.class);
    final int writeBufferHighWaterMark =
        injector
            .getNamedInstance(MyriaGlobalConfigurationModule.FlowControlWriteBufferHighMarkBytes.class);

    bootstrap.setOption("child.connectTimeoutMillis", connectTimeoutMillis);
    bootstrap.setOption("child.sendBufferSize", sendBufferSize);
    bootstrap.setOption("child.receiveBufferSize", receiveBufferSize);
    bootstrap.setOption("child.writeBufferLowWaterMark", writeBufferLowWaterMark);
    bootstrap.setOption("child.writeBufferHighWaterMark", writeBufferHighWaterMark);
    bootstrap.setOption("readWriteFair", true);

    return bootstrap;
  }

  /**
   * @return A pre-configured IPC server bootstrap for Workers.
   * @param worker the owner worker
   * @throws ConfigFileException
   * @throws NumberFormatException
   */
  public static ServerBootstrap createWorkerIPCServerBootstrap(final Injector injector)
      throws InjectionException {
    final ServerBootstrap bootstrap = new ServerBootstrap();

    bootstrap.setOption("reuseAddress", true);
    bootstrap.setOption("child.tcpNoDelay", true);
    bootstrap.setOption("child.keepAlive", false);
    bootstrap.setOption("child.reuseAddress", true);

    final int connectTimeoutMillis =
        injector.getNamedInstance(MyriaGlobalConfigurationModule.TcpConnectionTimeoutMillis.class);
    final int sendBufferSize =
        injector.getNamedInstance(MyriaGlobalConfigurationModule.TcpSendBufferSizeBytes.class);
    final int receiveBufferSize =
        injector.getNamedInstance(MyriaGlobalConfigurationModule.TcpReceiveBufferSizeBytes.class);
    final int writeBufferLowWaterMark =
        injector
            .getNamedInstance(MyriaGlobalConfigurationModule.FlowControlWriteBufferLowMarkBytes.class);
    final int writeBufferHighWaterMark =
        injector
            .getNamedInstance(MyriaGlobalConfigurationModule.FlowControlWriteBufferHighMarkBytes.class);

    bootstrap.setOption("child.connectTimeoutMillis", connectTimeoutMillis);
    bootstrap.setOption("child.sendBufferSize", sendBufferSize);
    bootstrap.setOption("child.receiveBufferSize", receiveBufferSize);
    bootstrap.setOption("child.writeBufferLowWaterMark", writeBufferLowWaterMark);
    bootstrap.setOption("child.writeBufferHighWaterMark", writeBufferHighWaterMark);

    bootstrap.setOption("readWriteFair", true);

    return bootstrap;
  }

  /** Prevent construction of utility class. */
  private IPCConfigurations() {}

}

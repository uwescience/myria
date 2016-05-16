package edu.washington.escience.myria.ipc;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import edu.washington.escience.myria.parallel.SocketInfo;
import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myria.parallel.ipc.IPCMessage;
import edu.washington.escience.myria.parallel.ipc.InJVMLoopbackChannelSink;
import edu.washington.escience.myria.parallel.ipc.PayloadSerializer;
import edu.washington.escience.myria.parallel.ipc.QueueBasedShortMessageProcessor;

/**
 * Holds utility functions for IPC testing.
 */
public class IPCTestUtil {

  public final static <PAYLOAD> IPCConnectionPool startIPCConnectionPool(
      final int myID,
      final HashMap<Integer, SocketInfo> computingUnits,
      final LinkedBlockingQueue<IPCMessage.Data<PAYLOAD>> shortMessageQueue,
      final PayloadSerializer ps,
      final int inputBufferCapacity,
      final int recoverTrigger,
      final int numNettyWorker)
      throws Exception {
    final IPCConnectionPool connectionPool =
        new IPCConnectionPool(
            myID,
            computingUnits,
            new ServerBootstrap(),
            new ClientBootstrap(),
            ps,
            new QueueBasedShortMessageProcessor<PAYLOAD>(shortMessageQueue),
            inputBufferCapacity,
            recoverTrigger);

    ExecutorService bossExecutor = Executors.newCachedThreadPool();
    ExecutorService workerExecutor = Executors.newCachedThreadPool();

    ChannelFactory clientChannelFactory =
        new NioClientSocketChannelFactory(bossExecutor, workerExecutor, numNettyWorker);

    ChannelFactory serverChannelFactory =
        new NioServerSocketChannelFactory(bossExecutor, workerExecutor, numNettyWorker);

    ChannelPipelineFactory serverPipelineFactory =
        new TestIPCPipelineFactories.ServerPipelineFactory(connectionPool, null);
    ChannelPipelineFactory clientPipelineFactory =
        new TestIPCPipelineFactories.ClientPipelineFactory(connectionPool);

    ChannelPipelineFactory inJVMPipelineFactory =
        new TestIPCPipelineFactories.InJVMPipelineFactory(connectionPool);

    connectionPool.start(
        serverChannelFactory,
        serverPipelineFactory,
        clientChannelFactory,
        clientPipelineFactory,
        inJVMPipelineFactory,
        new InJVMLoopbackChannelSink());
    return connectionPool;
  }
}

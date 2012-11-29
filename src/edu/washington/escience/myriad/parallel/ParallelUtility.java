package edu.washington.escience.myriad.parallel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import edu.washington.escience.myriad.parallel.Worker.MessageWrapper;

/**
 * Utility methods.
 */
public final class ParallelUtility {

  /** Prevent construction of utility class. */
  private ParallelUtility() {
  }

  public static int numBinaryOnesInInteger(int v) {
    int result = 0;
    while (v != 0) {
      result += (v & 0x01);
      v = v >>> 1;
    }
    return result;
  }

  /**
   * @param dest will be replaced if exists and override
   */
  public static void copyFileFolder(final File source, final File dest, final boolean override) throws IOException {
    if (dest.exists()) {
      if (!override) {
        return;
      } else {
        deleteFileFolder(dest);
      }
    }

    if (source.isDirectory()) {
      dest.mkdirs();
      final File[] children = source.listFiles();
      for (final File child : children) {
        copyFileFolder(child, new File(dest.getAbsolutePath() + "/" + child.getName()), override);
      }
    } else {
      InputStream in = null;
      OutputStream out = null;
      try {
        in = new FileInputStream(source);
        out = new FileOutputStream(dest);

        // Transfer bytes from in to out
        final byte[] buf = new byte[1024];
        int len;
        while ((len = in.read(buf)) > 0) {
          out.write(buf, 0, len);
        }
      } finally {
        if (in != null) {
          in.close();
        }
        if (out != null) {
          out.close();
        }
      }
    }
  }

  /**
   * Create a server side acceptor.
   */
  public static ServerBootstrap createMasterIPCServer(final LinkedBlockingQueue<MessageWrapper> messageQueue) {

    // Start server with Nb of active threads = 2*NB CPU + 1 as maximum.
    ChannelFactory factory =
        new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool(), Runtime
            .getRuntime().availableProcessors() * 2 + 1);

    ServerBootstrap bootstrap = new ServerBootstrap(factory);
    // OrderedMemoryAwareThreadPoolExecutor pipelineExecutor =
    // new OrderedMemoryAwareThreadPoolExecutor(200, 1048576, 1073741824, 100, TimeUnit.MILLISECONDS, Executors
    // .defaultThreadFactory());

    bootstrap.setPipelineFactory(new IPCPipelineFactories.MasterClientPipelineFactory(messageQueue));

    // ExecutionHandler eh;

    bootstrap.setOption("child.tcpNoDelay", true);
    bootstrap.setOption("child.keepAlive", false);
    bootstrap.setOption("child.reuseAddress", true);
    bootstrap.setOption("child.connectTimeoutMillis", 3000);
    bootstrap.setOption("child.sendBufferSize", 512 * 1024);
    bootstrap.setOption("child.receiveBufferSize", 512 * 1024);
    bootstrap.setOption("child.writeBufferLowWaterMark", 16 * 1024);
    bootstrap.setOption("child.writeBufferHighWaterMark", 256 * 1024);

    bootstrap.setOption("readWriteFair", true);

    return bootstrap;
  }

  /**
   * Create a server side acceptor.
   */
  public static ServerBootstrap createWorkerIPCServer(final LinkedBlockingQueue<MessageWrapper> messageQueue) {

    // Start server with Nb of active threads = 2*NB CPU + 1 as maximum.
    ChannelFactory factory =
        new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool(), Runtime
            .getRuntime().availableProcessors() * 2 + 1);

    ServerBootstrap bootstrap = new ServerBootstrap(factory);
    // Create the global ChannelGroup
    // ChannelGroup channelGroup = new DefaultChannelGroup(PongSerializeServer.class.getName());
    // Create the blockingQueue to wait for a limited number of client
    // 200 threads max, Memory limitation: 1MB by channel, 1GB global, 100 ms of timeout
    // OrderedMemoryAwareThreadPoolExecutor pipelineExecutor =
    // new OrderedMemoryAwareThreadPoolExecutor(200, 1048576, 1073741824, 100, TimeUnit.MILLISECONDS, Executors
    // .defaultThreadFactory());

    bootstrap.setPipelineFactory(new IPCPipelineFactories.MasterClientPipelineFactory(messageQueue));

    // ExecutionHandler eh;

    bootstrap.setOption("child.tcpNoDelay", true);
    bootstrap.setOption("child.keepAlive", false);
    bootstrap.setOption("child.reuseAddress", true);
    bootstrap.setOption("child.connectTimeoutMillis", 3000);
    bootstrap.setOption("child.sendBufferSize", 512 * 1024);
    bootstrap.setOption("child.receiveBufferSize", 512 * 1024);
    bootstrap.setOption("child.writeBufferLowWaterMark", 16 * 1024);
    bootstrap.setOption("child.writeBufferHighWaterMark", 256 * 1024);

    bootstrap.setOption("readWriteFair", true);

    return bootstrap;
  }

  /**
   * Create a client side connector to the server.
   * 
   * @param clientFactory All the client connections share the same generation factory, basically, they share the same
   *          thread pool
   */
  static ClientBootstrap createIPCClient(final ChannelFactory clientFactory, final ChannelPipelineFactory cpf) {

    // Create the bootstrap
    ClientBootstrap bootstrap = new ClientBootstrap(clientFactory);
    bootstrap.setPipelineFactory(cpf);
    bootstrap.setOption("tcpNoDelay", true);
    bootstrap.setOption("keepAlive", false);
    bootstrap.setOption("reuseAddress", true);
    bootstrap.setOption("connectTimeoutMillis", 3000);
    bootstrap.setOption("sendBufferSize", 512 * 1024);
    bootstrap.setOption("receiveBufferSize", 512 * 1024);
    bootstrap.setOption("writeBufferLowWaterMark", 16 * 1024);
    bootstrap.setOption("writeBufferHighWaterMark", 256 * 1024);

    return bootstrap;
  }

  public static void deleteFileFolder(final File f) throws IOException {
    if (!f.exists()) {
      return;
    }
    if (f.isDirectory()) {
      for (final File c : f.listFiles()) {
        deleteFileFolder(c);
      }
    }
    if (!f.delete()) {
      throw new FileNotFoundException("Failed to delete file: " + f);
    }
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

  /**
   * Shutdown the java virtual machine.
   */
  public static void shutdownVM() {
    System.exit(0);
  }

  /**
   * Unbind the acceptor from the binded port and close all the connections.
   */
  public static void shutdownIPC(final Channel ipcServerChannel, final IPCConnectionPool connectionPool) {
    connectionPool.shutdown();
    ipcServerChannel.disconnect();
    ipcServerChannel.close();
    ipcServerChannel.unbind();

    ipcServerChannel.getFactory().releaseExternalResources();
  }

  public static void writeFile(final File f, final String content) throws IOException {
    final FileOutputStream o = new FileOutputStream(f);
    o.write(content.getBytes());
    o.close();
  }

}

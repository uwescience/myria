package edu.washington.escience.myriad.parallel;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Map;

import org.apache.mina.core.future.CloseFuture;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.future.WriteFuture;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.serialization.ObjectSerializationCodecFactory;
import org.apache.mina.filter.compression.CompressionFilter;
import org.apache.mina.transport.socket.SocketSessionConfig;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.mina.transport.socket.nio.NioSocketConnector;

/**
 * Utility methods
 * */
public class ParallelUtility {

  /**
   * Shutdown the java virtual machine
   * */
  public static void shutdownVM() {
    System.exit(0);
  }

  /**
   * create a client side connector to the server
   * */
  private static IoConnector createConnector() {
    IoConnector connector = new NioSocketConnector();
    SocketSessionConfig config = (SocketSessionConfig) connector.getSessionConfig();
    config.setKeepAlive(true); // true?
    // No delay
    config.setTcpNoDelay(true);
    config.setIdleTime(IdleStatus.BOTH_IDLE, 5);
    config.setReceiveBufferSize(2048);
    config.setSendBufferSize(2048);
    config.setReadBufferSize(2048);

    connector.getFilterChain().addLast("compressor", new CompressionFilter());

    connector.getFilterChain().addLast("codec", new ProtocolCodecFilter(new ObjectSerializationCodecFactory()));

    connector.setHandler(new IoHandlerAdapter() {
      public void exceptionCaught(IoSession session, Throwable cause) {
        cause.printStackTrace();
      }

      public void messageReceived(IoSession session, Object message) throws Exception {
        System.out.println("Default IOHandler, Message received: " + message);
        super.messageReceived(session, message);
      }
    });
    return connector;
  }

  /**
   * Close a session. Every time a session is to be closed, do call this method. Do not directly call session.close;
   * */
  public static CloseFuture closeSession(IoSession session) {
    if (session == null)
      return null;
    CloseFuture cf = session.close(false);
    IoConnector ic = (IoConnector) session.getAttribute("connecter");

    if (ic != null) {
      Map<Long, IoSession> activeSessions = ic.getManagedSessions();
      if ((activeSessions.containsValue(session) && activeSessions.size() <= 1) || activeSessions.size() <= 0)
        ic.dispose(false);
    }
    return cf;
  }

  /**
   * Create a session for network communication.
   * 
   * @return An IoSession is a logical connection between a server and a client in Apache Mina. A set of sessions may
   *         share the same underlying TCP/UDP connection.
   * 
   * @param remoteAddress The address of the remote server
   * 
   * @param ioHandler The handler which processes received messages from the returned session
   * 
   * @param connectionTimeoutMS The timeout of connecting the server in milliseconds.
   * */
  public static IoSession createSession(SocketAddress remoteAddress, IoHandlerAdapter ioHandler,
      long connectionTimeoutMS) {

    IoSession session = null;

    IoConnector ic = null;
    ic = createConnector();
    ic.setHandler(ioHandler);
    ConnectFuture c = ic.connect(remoteAddress);
    boolean connected = false;
    if (connectionTimeoutMS > 0)
      connected = c.awaitUninterruptibly(connectionTimeoutMS);
    else
      connected = c.awaitUninterruptibly().isConnected();
    if (connected) {
      session = c.getSession();
      session.setAttribute("connecter", ic);
      return session;
    }
    return session;
  }

  /**
   * create a server side acceptor
   * */
  public static NioSocketAcceptor createAcceptor() {
    NioSocketAcceptor acceptor = new NioSocketAcceptor(10);

    SocketSessionConfig config = (SocketSessionConfig) acceptor.getSessionConfig();
    config.setKeepAlive(false);
    config.setTcpNoDelay(true);
    /**
     * A session without any write/read actions in 5 seconds is assumed to be idle
     * */
    config.setIdleTime(IdleStatus.BOTH_IDLE, 5);
    config.setReceiveBufferSize(2048);
    config.setSendBufferSize(2048);
    config.setReadBufferSize(2048);

    acceptor.setCloseOnDeactivation(true);

    acceptor.getFilterChain().addLast("compressor", new CompressionFilter());
    acceptor.getFilterChain().addLast("codec", new ProtocolCodecFilter(new ObjectSerializationCodecFactory()));
    acceptor.setCloseOnDeactivation(true);

    acceptor.setHandler(new IoHandlerAdapter() {
      public void exceptionCaught(IoSession session, Throwable cause) {
        cause.printStackTrace();
      }

      public void messageReceived(IoSession session, Object message) throws Exception {
        System.out.println("Default IOHandler, Message received: " + message);
        super.messageReceived(session, message);
      }
    });
    acceptor.setReuseAddress(true);

    return acceptor;
  }

  /**
   * unbind the acceptor from the binded port and close all the connections
   * */
  public static void unbind(NioSocketAcceptor acceptor) {

    for (IoSession session : acceptor.getManagedSessions().values()) {
      session.close(true);
    }

    while (acceptor.isActive() || !acceptor.isDisposed()) {
      acceptor.unbind();
      acceptor.dispose(false);
    }
  }

  public static void broadcastMessageToWorkers(Object message, SocketInfo[] workers, IoHandlerAdapter handler,
      long timeoutMS) {

    for (SocketInfo worker : workers) {
      IoSession s = ParallelUtility.createSession(worker.getAddress(), handler, timeoutMS);
      if (s != null) {
        s.write(message).addListener(new IoFutureListener<WriteFuture>() {

          @Override
          public void operationComplete(WriteFuture future) {
            ParallelUtility.closeSession(future.getSession());
          }
        });
      }
    }

  }

  public static String[] removeArg(String[] args, int toBeRemoved) {
    if (args == null)
      return null;

    if (toBeRemoved < 0 || toBeRemoved >= args.length)
      return args;
    String[] newArgs = new String[args.length - 1];
    System.arraycopy(args, 0, newArgs, 0, toBeRemoved);
    System.arraycopy(args, toBeRemoved + 1, newArgs, toBeRemoved, args.length - toBeRemoved - 1);
    return newArgs;
  }

  public static void deleteFileFolder(File f) throws IOException {
    if (!f.exists())
      return;
    if (f.isDirectory()) {
      for (File c : f.listFiles())
        deleteFileFolder(c);
    }
    if (!f.delete())
      throw new FileNotFoundException("Failed to delete file: " + f);
  }

  public static void writeFile(File f, String content) throws IOException {
    FileOutputStream o = new FileOutputStream(f);
    o.write(content.getBytes());
    o.close();
  }

  /**
   * @param dest will be replaced if exists and override
   * */
  public static void copyFileFolder(File source, File dest, boolean override) throws IOException {
    if (dest.exists())
      if (!override)
        return;
      else
        deleteFileFolder(dest);

    if (source.isDirectory()) {
      dest.mkdirs();
      File[] children = source.listFiles();
      for (File child : children)
        copyFileFolder(child, new File(dest.getAbsolutePath() + "/" + child.getName()), override);
    } else {
      InputStream in = null;
      OutputStream out = null;
      try {
        in = new FileInputStream(source);
        out = new FileOutputStream(dest);

        // Transfer bytes from in to out
        byte[] buf = new byte[1024];
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
}

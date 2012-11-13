package edu.washington.escience.myriad.parallel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketAddress;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.mina.core.future.CloseFuture;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.protobuf.ProtobufCodecFactory;
import org.apache.mina.filter.compression.CompressionFilter;
import org.apache.mina.transport.socket.SocketSessionConfig;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;

/**
 * Utility methods.
 */
public class ParallelUtility {

  public static int numBinaryOnesInInteger(int v) {
    int result = 0;
    while (v != 0) {
      result += (v & 0x01);
      v = v >>> 1;
    }
    return result;
  }

  /**
   * Close a session. Every time a session is to be closed, do call this method. Do not directly call session.close;.
   */
  public static CloseFuture closeSession(final IoSession session) {
    if (session == null) {
      return null;
    }
    final CloseFuture cf = session.close(false);
    final IoConnector ic = (IoConnector) session.getAttribute("connector");

    if (ic != null) {
      final Map<Long, IoSession> activeSessions = ic.getManagedSessions();
      if ((activeSessions.containsValue(session) && activeSessions.size() <= 1) || activeSessions.size() <= 0) {
        ic.dispose(false);
      }
    }
    return cf;
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
  public static NioSocketAcceptor createAcceptor() {
    final NioSocketAcceptor acceptor = new NioSocketAcceptor(10);

    final SocketSessionConfig config = acceptor.getSessionConfig();
    config.setKeepAlive(false);
    config.setTcpNoDelay(true);
    /**
     * A session without any write/read actions in 1 hour is assumed to be idle
     */
    config.setIdleTime(IdleStatus.BOTH_IDLE, 3600);
    config.setReceiveBufferSize(2048);
    config.setSendBufferSize(2048);
    config.setReadBufferSize(2048);

    acceptor.setCloseOnDeactivation(true);

    acceptor.getFilterChain().addLast("compressor", new CompressionFilter());

    acceptor.getFilterChain().addLast("codec",
        new ProtocolCodecFilter(ProtobufCodecFactory.newInstance(TransportMessage.getDefaultInstance())));
    acceptor.setCloseOnDeactivation(true);

    acceptor.setHandler(new IoHandlerAdapter() {
      @Override
      public void exceptionCaught(final IoSession session, final Throwable cause) {
        cause.printStackTrace();
      }

      @Override
      public void messageReceived(final IoSession session, final Object message) throws Exception {
        System.out.println("Default IOHandler, Message received: " + message);
        super.messageReceived(session, message);
      }
    });
    acceptor.setReuseAddress(true);

    return acceptor;
  }

  /**
   * Create a client side connector to the server.
   */
  private static IoConnector createConnector() {
    final IoConnector connector = new NioSocketConnector();
    final SocketSessionConfig config = (SocketSessionConfig) connector.getSessionConfig();
    config.setKeepAlive(true); // true?
    // No delay
    config.setTcpNoDelay(true);
    config.setIdleTime(IdleStatus.BOTH_IDLE, 5);
    config.setReceiveBufferSize(2048);
    config.setSendBufferSize(2048);
    config.setReadBufferSize(2048);

    connector.getFilterChain().addLast("compressor", new CompressionFilter());

    connector.getFilterChain().addLast("codec",
        new ProtocolCodecFilter(ProtobufCodecFactory.newInstance(TransportMessage.getDefaultInstance())));

    connector.setHandler(new IoHandlerAdapter() {
      @Override
      public void exceptionCaught(final IoSession session, final Throwable cause) {
        cause.printStackTrace();
      }

      @Override
      public void messageReceived(final IoSession session, final Object message) throws Exception {
        System.out.println("Default IOHandler, Message received: " + message);
        super.messageReceived(session, message);
      }
    });
    return connector;
  }

  public static IoSession createSession(final SocketAddress remoteAddress, final IoHandler ioHandler,
      final long connectionTimeoutMS) {

    IoSession session = null;

    IoConnector ic = null;
    ic = createConnector();
    ic.setHandler(ioHandler);
    final ConnectFuture c = ic.connect(remoteAddress);
    boolean connected = false;
    if (connectionTimeoutMS > 0) {
      connected = c.awaitUninterruptibly(connectionTimeoutMS);
    } else {
      connected = c.awaitUninterruptibly().isConnected();
    }
    if (connected) {
      session = c.getSession();
      session.setAttribute("connector", ic);
      return session;
    }
    return session;
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
  public static void unbind(final NioSocketAcceptor acceptor) {

    for (final IoSession session : acceptor.getManagedSessions().values()) {
      session.close(true);
    }

    while (acceptor.isActive() || !acceptor.isDisposed()) {
      acceptor.unbind();
      acceptor.dispose(false);
    }
  }

  public static void writeFile(final File f, final String content) throws IOException {
    final FileOutputStream o = new FileOutputStream(f);
    o.write(content.getBytes());
    o.close();
  }

  public static String[] readEclipseClasspath(final File eclipseClasspathXMLFile) throws IOException {

    final DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder;
    try {
      dBuilder = dbFactory.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      throw new IOException(e);
    }

    Document doc;
    try {
      doc = dBuilder.parse(eclipseClasspathXMLFile);
    } catch (SAXException e) {
      throw new IOException(e);
    }
    doc.getDocumentElement().normalize();

    final NodeList nList = doc.getElementsByTagName("classpathentry");

    final String separator = System.getProperty("path.separator");
    final StringBuilder classpathSB = new StringBuilder();
    for (int i = 0; i < nList.getLength(); i++) {
      final Node node = nList.item(i);
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        final Element e = (Element) node;

        final String kind = e.getAttribute("kind");
        if (kind.equals("output")) {
          classpathSB.append(new File(e.getAttribute("path")).getAbsolutePath() + separator);
        }
        if (kind.equals("lib")) {
          classpathSB.append(new File(e.getAttribute("path")).getAbsolutePath() + separator);
        }
      }
    }
    final NodeList attributeList = doc.getElementsByTagName("attribute");
    final StringBuilder libPathSB = new StringBuilder();
    for (int i = 0; i < attributeList.getLength(); i++) {
      final Node node = attributeList.item(i);
      String value = null;
      if (node.getNodeType() == Node.ELEMENT_NODE
          && ("org.eclipse.jdt.launching.CLASSPATH_ATTR_LIBRARY_PATH_ENTRY".equals(((Element) node)
              .getAttribute("name"))) && ((value = ((Element) node).getAttribute("value")) != null)) {
        File f = new File(value);
        while (value != null && value.length() > 0 && !f.exists()) {
          value = value.substring(value.indexOf(File.separator) + 1);
          f = new File(value);
        }
        if (f.exists()) {
          libPathSB.append(f.getAbsolutePath() + separator);
        }
      }
    }
    return new String[] { classpathSB.toString(), libPathSB.toString() };
  }
}

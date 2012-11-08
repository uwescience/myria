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
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import edu.washington.escience.myriad.parallel.Worker.MessageWrapper;

/**
 * Utility methods.
 */
public class ParallelUtility {

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
  public static ServerBootstrap createAcceptor(final LinkedBlockingQueue<MessageWrapper> messageBuffer) {

    // Start server with Nb of active threads = 2*NB CPU + 1 as maximum.
    ChannelFactory factory =
        new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool(), Runtime
            .getRuntime().availableProcessors() * 2 + 1);

    ServerBootstrap bootstrap = new ServerBootstrap(factory);
    // Create the global ChannelGroup
    // ChannelGroup channelGroup = new DefaultChannelGroup(PongSerializeServer.class.getName());
    // Create the blockingQueue to wait for a limited number of client
    // 200 threads max, Memory limitation: 1MB by channel, 1GB global, 100 ms of timeout
    OrderedMemoryAwareThreadPoolExecutor pipelineExecutor =
        new OrderedMemoryAwareThreadPoolExecutor(200, 1048576, 1073741824, 100, TimeUnit.MILLISECONDS, Executors
            .defaultThreadFactory());

    bootstrap.setPipelineFactory(new IPCPipelineFactories.MasterClientPipelineFactory(messageBuffer));

    ExecutionHandler eh;

    bootstrap.setOption("child.tcpNoDelay", true);
    bootstrap.setOption("child.keepAlive", false);
    bootstrap.setOption("child.reuseAddress", true);
    bootstrap.setOption("child.connectTimeoutMillis", 3000);
    bootstrap.setOption("child.sendBufferSize", 512 * 1024 * 1024);
    bootstrap.setOption("child.receiveBufferSize", 512 * 1024 * 1024);

    bootstrap.setOption("readWriteFair", true);

    return bootstrap;
  }

  /**
   * Create a client side connector to the server.
   */
  static ClientBootstrap createConnector(final LinkedBlockingQueue<MessageWrapper> messageBuffer) {

    // Start client with Nb of active threads = 3 as maximum.
    ChannelFactory factory =
        new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool(), 3);
    // Create the bootstrap
    ClientBootstrap bootstrap = new ClientBootstrap(factory);
    bootstrap.setPipelineFactory(new IPCPipelineFactories.WorkerClientPipelineFactory(messageBuffer));
    bootstrap.setOption("tcpNoDelay", true);
    bootstrap.setOption("keepAlive", false);
    bootstrap.setOption("reuseAddress", true);
    bootstrap.setOption("connectTimeoutMillis", 3000);
    bootstrap.setOption("sendBufferSize", 512 * 1024 * 1024);
    bootstrap.setOption("receiveBufferSize", 512 * 1024 * 1024);

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

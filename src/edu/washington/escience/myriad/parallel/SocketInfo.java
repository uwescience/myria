package edu.washington.escience.myriad.parallel;

import java.io.Serializable;
import java.net.InetSocketAddress;

/**
 * A simple wrapper that wraps the socket information of both workers and the server (coordinator).
 */
public class SocketInfo implements Serializable {

  private static final long serialVersionUID = 1L;
  private final String host;
  private final int port;
  private transient InetSocketAddress address;
  private transient String id;

  public SocketInfo(final String host, final int port) {
    this.host = host;
    this.port = port;
  }

  public InetSocketAddress getAddress() {
    if (address == null) {
      address = new InetSocketAddress(host, port);
    }
    return address;
  }

  public String getHost() {
    return host;
  }

  public String getId() {
    if (id == null) {
      id = host + ":" + port;
    }
    return id;
  }

  public int getPort() {
    return port;
  }

  @Override
  public String toString() {
    return getAddress().toString();
  }

}
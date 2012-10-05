package edu.washington.escience.myriad.parallel;

import java.io.Serializable;
import java.net.InetSocketAddress;

/**
 * A simple wrapper that wraps the socket information of both workers and the server (coordinator).
 * */
public class SocketInfo implements Serializable {

  private static final long serialVersionUID = 1L;
  private String host;
  private int port;
  private transient InetSocketAddress address;
  private transient String id;

  public SocketInfo(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getId() {
    if (id == null)
      id = host + ":" + port;
    return id;
  }

  public InetSocketAddress getAddress() {
    if (address == null)
      address = new InetSocketAddress(host, port);
    return address;
  }

  public String toString() {
    return address.toString();
  }

}
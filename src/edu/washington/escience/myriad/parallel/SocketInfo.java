package edu.washington.escience.myriad.parallel;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Objects;

import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * A simple wrapper that wraps the socket information of both workers and the server (coordinator).
 */
public class SocketInfo implements Serializable {
  /** Class-specific magic number used to generate the hash code. */
  private static final int MAGIC_HASHCODE1 = 365;
  /** Class-specific magic number used to generate the hash code. */
  private static final int MAGIC_HASHCODE2 = 91;
  /** The hash code of this immutable SocketInfo. */
  private Integer myHashCode = null;

  private static final long serialVersionUID = 1L;
  private final String host;
  private final int port;
  private transient InetSocketAddress address;
  private transient String id;

  /**
   * Create a SocketInfo object from a string in the format host:port.
   * 
   * @param hostPort a string in the format (host:port) that describes a socket.
   * @return a SocketInfo corresponding to the given string.
   */
  public static SocketInfo valueOf(final String hostPort) {
    Objects.requireNonNull(hostPort);
    final String[] parts = hostPort.split(":");
    if (parts.length != 2) {
      throw new IllegalArgumentException("argument is not of format (host:port)");
    }
    return new SocketInfo(parts[0], Integer.parseInt(parts[1]));
  }

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
  public final String toString() {
    return getAddress().toString();
  }

  @Override
  public final boolean equals(final Object other) {
    if (!(other instanceof SocketInfo)) {
      return false;
    }
    SocketInfo sockInfo = (SocketInfo) other;
    return (sockInfo.host.equals(host)) && (sockInfo.port == port);
  }

  @Override
  public final int hashCode() {
    /* If cached, use the cached version. */
    if (myHashCode != null) {
      return myHashCode;
    }
    /* Compute and cache the hash code. */
    final HashCodeBuilder hb = new HashCodeBuilder(MAGIC_HASHCODE1, MAGIC_HASHCODE2);
    hb.append(host).append(port);
    int hash = hb.toHashCode();
    myHashCode = hash;
    return hash;
  }
}
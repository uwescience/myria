package edu.washington.escience.myriad.parallel;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Objects;

import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * A simple wrapper that wraps the socket information of both workers and the server (coordinator).
 */
public final class SocketInfo implements Serializable {
  /** Class-specific magic number used to generate the hash code. */
  private static final int MAGIC_HASHCODE1 = 947;
  /** Class-specific magic number used to generate the hash code. */
  private static final int MAGIC_HASHCODE2 = 91;

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

  /** The hash code of this immutable SocketInfo. */
  private Integer myHashCode = null;
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The host (name or IP) of the network connection. */
  private final String host;
  /** The port of the connection. */
  private final int port;
  /** An Address that holds these info. */
  private transient InetSocketAddress address;

  /** A String host:port. */
  private transient String hostPortString;

  /**
   * 
   * @param host the name or IPv4 of the network address.
   * @param port as usual, 16-bit port.
   */
  public SocketInfo(final String host, final int port) {
    Objects.requireNonNull(host);
    Objects.requireNonNull(port);
    this.host = host;
    this.port = port;
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof SocketInfo)) {
      return false;
    }
    final SocketInfo sockInfo = (SocketInfo) other;
    return (sockInfo.host.equals(host)) && (sockInfo.port == port);
  }

  /**
   * @return an InetSocketAddress that describes the given host:port address.
   */
  public InetSocketAddress getAddress() {
    if (address == null) {
      address = new InetSocketAddress(host, port);
    }
    return address;
  }

  /**
   * @return the host string.
   */
  public String getHost() {
    return host;
  }

  /**
   * @return the port.
   */
  public int getPort() {
    return port;
  }

  @Override
  public int hashCode() {
    /* If cached, use the cached version. */
    if (myHashCode != null) {
      return myHashCode;
    }
    /* Compute and cache the hash code. */
    final HashCodeBuilder hb = new HashCodeBuilder(MAGIC_HASHCODE1, MAGIC_HASHCODE2);
    hb.append(host).append(port);
    final int hash = hb.toHashCode();
    myHashCode = hash;
    return hash;
  }

  @Override
  public String toString() {
    if (hostPortString == null) {
      hostPortString = host + ":" + port;
    }
    return hostPortString;
  }
}
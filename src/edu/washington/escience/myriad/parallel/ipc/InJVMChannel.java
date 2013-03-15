package edu.washington.escience.myriad.parallel.ipc;

import static org.jboss.netty.channel.Channels.fireChannelClosed;
import static org.jboss.netty.channel.Channels.fireChannelDisconnected;
import static org.jboss.netty.channel.Channels.fireChannelUnbound;

import java.net.SocketAddress;

import org.jboss.netty.channel.AbstractChannel;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelConfig;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelSink;
import org.jboss.netty.channel.Channels;
import org.slf4j.LoggerFactory;

public class InJVMChannel extends AbstractChannel {

  /** The logger for this class. */
  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(InJVMChannel.class.getName());

  public InJVMChannel(final ChannelPipeline localInJVMPipeline, final ChannelSink localInJVMChannelSink) {
    super(null, null, localInJVMPipeline, localInJVMChannelSink);
    Channels.fireChannelOpen(this);
  }

  @Override
  public ChannelFuture bind(final SocketAddress localAddress) {
    throw new UnsupportedOperationException();
  }

  void closeNow(ChannelFuture cf) {
    // Close the self.
    if (!setClosed()) {
      return;
    }

    fireChannelDisconnected(this);
    fireChannelUnbound(this);
    fireChannelClosed(this);

    cf.setSuccess();
  }

  @Override
  public ChannelFuture setInterestOps(final int interestOps) {
    int newIOPS = interestOps;
    if ((interestOps & OP_READ) == 0) {// server side stop read => client side stop write.
      newIOPS = interestOps | Channel.OP_WRITE;
    } else {
      newIOPS = interestOps & ~Channel.OP_WRITE;
    }
    setInterestOpsNow(newIOPS);
    return super.setInterestOps(newIOPS);
  }

  @Override
  public ChannelFuture connect(final SocketAddress remoteAddress) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ChannelFuture disconnect() {
    return close();
  }

  @Override
  public ChannelFuture unbind() {
    return close();
  }

  @Override
  public boolean isBound() {
    return isOpen();
  }

  @Override
  public boolean isConnected() {
    return isOpen();
  }

  @Override
  public ChannelFuture write(final Object message, final SocketAddress remoteAddress) {
    return write(message);
  }

  @Override
  public ChannelConfig getConfig() {
    return null;
  }

  @Override
  public SocketAddress getLocalAddress() {
    return null;
  }

  @Override
  public SocketAddress getRemoteAddress() {
    return null;
  }

}

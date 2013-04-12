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

/**
 * The Channel used when the sender and the receiver are actually in the same JVM.
 * */
public class InJVMChannel extends AbstractChannel {

  /** The logger for this class. */
  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(InJVMChannel.class.getName());

  /**
   * Messages will be processed by the pipeline first and if the pipeline decides not to filter out a message, then it
   * will finally reach the sink.
   * 
   * @param localInJVMPipeline the pipeline to execute when events happen on this channel.
   * @param localInJVMChannelSink the message sink
   * */
  public InJVMChannel(final ChannelPipeline localInJVMPipeline, final ChannelSink localInJVMChannelSink) {
    super(null, null, localInJVMPipeline, localInJVMChannelSink);
    Channels.fireChannelOpen(this);
  }

  @Override
  public final ChannelFuture bind(final SocketAddress localAddress) {
    throw new UnsupportedOperationException();
  }

  /**
   * close the channel. Should be called by the sink.
   * 
   * @param cf the close channel future.
   * */
  final void closeNow(final ChannelFuture cf) {
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
  public final ChannelFuture setInterestOps(final int interestOps) {
    int newIOPS = interestOps;
    if ((interestOps & OP_READ) == 0) {
      // server side stop read => client side stop write.
      newIOPS = interestOps | Channel.OP_WRITE;
    } else {
      newIOPS = interestOps & ~Channel.OP_WRITE;
    }
    setInterestOpsNow(newIOPS);
    return super.setInterestOps(newIOPS);
  }

  @Override
  public final ChannelFuture connect(final SocketAddress remoteAddress) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final ChannelFuture disconnect() {
    return close();
  }

  @Override
  public final ChannelFuture unbind() {
    return close();
  }

  @Override
  public final boolean isBound() {
    return isOpen();
  }

  @Override
  public final boolean isConnected() {
    return isOpen();
  }

  @Override
  public final ChannelFuture write(final Object message, final SocketAddress remoteAddress) {
    return write(message);
  }

  @Override
  public final ChannelConfig getConfig() {
    return null;
  }

  @Override
  public final SocketAddress getLocalAddress() {
    return null;
  }

  @Override
  public final SocketAddress getRemoteAddress() {
    return null;
  }

}

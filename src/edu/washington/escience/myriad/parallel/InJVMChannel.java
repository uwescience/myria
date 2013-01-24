package edu.washington.escience.myriad.parallel;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.nio.channels.NotYetConnectedException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.AbstractChannel;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelConfig;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipeline;

import edu.washington.escience.myriad.parallel.Worker.MessageWrapper;
import edu.washington.escience.myriad.util.IPCUtils;

public class InJVMChannel implements Channel {

  public class InJVMChannelCloseFuture extends InJVMChannelFuture {

    private final ConcurrentLinkedQueue<ChannelFutureListener> listeners;
    private volatile boolean done = false;
    private volatile Throwable cause = null;

    public InJVMChannelCloseFuture() {
      listeners = new ConcurrentLinkedQueue<ChannelFutureListener>();
    }

    @Override
    public void addListener(final ChannelFutureListener listener) {
      if (!done) {
        listeners.add(listener);
      } else {
        try {
          listener.operationComplete(this);
        } catch (final Exception e) {
          e.printStackTrace();
        }
      }
    }

    @Override
    public boolean cancel() {
      return false;
    }

    @Override
    public Throwable getCause() {
      return cause;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return done;
    }

    @Override
    public boolean isSuccess() {
      if (done && cause == null) {
        return true;
      } else {
        return false;
      }
    }

    @Override
    public void removeListener(final ChannelFutureListener listener) {
      listeners.remove(listener);
    }

    @Override
    public boolean setFailure(final Throwable cause) {
      if (!done) {
        this.cause = cause;
        done = true;
        for (final ChannelFutureListener cl : listeners) {
          try {
            cl.operationComplete(this);
          } catch (final Exception e) {
            e.printStackTrace();
          }
        }
        return true;
      }
      return false;
    }

    @Override
    public boolean setSuccess() {
      if (!done) {
        done = true;
        for (final ChannelFutureListener cl : listeners) {
          try {
            cl.operationComplete(this);
          } catch (final Exception e) {
            e.printStackTrace();
          }
        }
        return true;
      }
      return false;
    }
  }

  public class InJVMChannelFuture implements ChannelFuture {

    private final boolean success;
    private final Throwable cause;

    public InJVMChannelFuture() {
      success = true;
      cause = null;
    }

    public InJVMChannelFuture(final Throwable cause) {
      success = false;
      this.cause = cause;
    }

    @Override
    public void addListener(final ChannelFutureListener listener) {
      try {
        listener.operationComplete(this);
      } catch (final Exception e) {
        e.printStackTrace();
      }
    }

    @Override
    public ChannelFuture await() throws InterruptedException {
      return this;
    }

    @Override
    public boolean await(final long timeoutMillis) throws InterruptedException {
      return true;
    }

    @Override
    public boolean await(final long timeout, final TimeUnit unit) throws InterruptedException {
      return true;
    }

    @Override
    public ChannelFuture awaitUninterruptibly() {
      return this;
    }

    @Override
    public boolean awaitUninterruptibly(final long timeoutMillis) {
      return true;
    }

    @Override
    public boolean awaitUninterruptibly(final long timeout, final TimeUnit unit) {
      return true;
    }

    @Override
    public boolean cancel() {
      return false;
    }

    @Override
    public Throwable getCause() {
      return cause;
    }

    @Override
    public Channel getChannel() {
      return InJVMChannel.this;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public boolean isSuccess() {
      return success;
    }

    @Override
    public void removeListener(final ChannelFutureListener listener) {
    }

    @Override
    @Deprecated
    public ChannelFuture rethrowIfFailed() throws Exception {
      return this;
    }

    @Override
    public boolean setFailure(final Throwable cause) {
      return false;
    }

    @Override
    public boolean setProgress(final long amount, final long current, final long total) {
      return true;
    }

    @Override
    public boolean setSuccess() {
      return false;
    }

    @Override
    public ChannelFuture sync() throws InterruptedException {
      return this;
    }

    @Override
    public ChannelFuture syncUninterruptibly() {
      return this;
    }
  }

  private final LinkedBlockingQueue<MessageWrapper> dataQueue;
  private final ChannelFuture closeFuture;
  private final Integer channelID;
  private final int myIPCID;

  public InJVMChannel(final int myID, final LinkedBlockingQueue<MessageWrapper> dataQueue) {
    myIPCID = myID;
    this.dataQueue = dataQueue;
    closeFuture = new InJVMChannelCloseFuture();
    Method m;
    Integer idTmp = 0;
    try {
      m = AbstractChannel.class.getDeclaredMethod("allocateId", Channel.class);

      m.setAccessible(true); // if security settings allow this
      idTmp = (Integer) m.invoke(null, this); // use null if the method is static
    } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException e) {
      e.printStackTrace();
    }
    channelID = idTmp;
  }

  @Override
  public ChannelFuture bind(final SocketAddress localAddress) {
    return new InJVMChannelFuture();
  }

  @Override
  public ChannelFuture close() {
    closeFuture.setSuccess();
    return closeFuture;
  }

  /**
   * Compares the {@linkplain #getId() ID} of the two channels.
   */
  @Override
  public final int compareTo(final Channel o) {
    return getId().compareTo(o.getId());
  }

  @Override
  public ChannelFuture connect(final SocketAddress remoteAddress) {
    return new InJVMChannelFuture();
  }

  @Override
  public ChannelFuture disconnect() {
    return close();
  }

  /**
   * Returns {@code true} if and only if the specified object is identical with this channel (i.e: {@code this == o}).
   */
  @Override
  public final boolean equals(final Object o) {
    return this == o;
  }

  @Override
  public Object getAttachment() {
    return null;
  }

  @Override
  public ChannelFuture getCloseFuture() {
    return closeFuture;
  }

  @Override
  public ChannelConfig getConfig() {
    return null;
  }

  @Override
  public ChannelFactory getFactory() {
    return null;
  }

  @Override
  public Integer getId() {
    return channelID;
  }

  @Override
  public int getInterestOps() {
    return Channel.OP_WRITE;
  }

  @Override
  public SocketAddress getLocalAddress() {
    return null;
  }

  @Override
  public Channel getParent() {
    return null;
  }

  @Override
  public ChannelPipeline getPipeline() {
    return null;
  }

  @Override
  public SocketAddress getRemoteAddress() {
    return null;
  }

  /**
   * Returns the ID of this channel.
   */
  @Override
  public final int hashCode() {
    return channelID;
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
  public boolean isOpen() {
    return !closeFuture.isDone();
  }

  @Override
  public boolean isReadable() {
    return false;
  }

  @Override
  public boolean isWritable() {
    return true;
  }

  @Override
  public void setAttachment(final Object attachment) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ChannelFuture setInterestOps(final int interestOps) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ChannelFuture setReadable(final boolean readable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ChannelFuture unbind() {
    return close();
  }

  @Override
  public ChannelFuture write(final Object message) {
    if (isOpen()) {
      try {
        dataQueue.add(new MessageWrapper(myIPCID, IPCUtils.asTM(message)));
        return new InJVMChannelFuture();
      } catch (final Throwable e) {
        return new InJVMChannelFuture(e);
      }
    } else {
      return new InJVMChannelFuture(new NotYetConnectedException());
    }
  }

  @Override
  public ChannelFuture write(final Object message, final SocketAddress remoteAddress) {
    return write(message);
  }

}

package edu.washington.escience.myriad.parallel.ipc;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import edu.washington.escience.myriad.parallel.Producer;

/**
 * 
 * An {@link StreamOutputChannel} represents a partition of {@link Producer}.
 * 
 * @param <PAYLOAD> the type of payload that this output channel will send.
 * */
public class StreamOutputChannel<PAYLOAD> extends StreamIOChannel {

  /** The logger for this class. */
  static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(StreamOutputChannel.class.getName());

  /**
   * Output disabled listeners.
   * */
  private final ConcurrentLinkedQueue<IPCEventListener<StreamOutputChannel<PAYLOAD>>> outputDisableListeners;

  /**
   * Output recovered listeners.
   * */
  private final ConcurrentLinkedQueue<IPCEventListener<StreamOutputChannel<PAYLOAD>>> outputRecoverListeners;

  /**
   * the output disabled event.
   * */
  public final IPCEvent<StreamOutputChannel<PAYLOAD>> outputDisabledEvent =
      new IPCEvent<StreamOutputChannel<PAYLOAD>>() {

        @Override
        public StreamOutputChannel<PAYLOAD> getAttachment() {
          return StreamOutputChannel.this;
        }
      };

  /**
   * the output enabled event.
   * */
  public final IPCEvent<StreamOutputChannel<PAYLOAD>> outputEnabledEvent =
      new IPCEvent<StreamOutputChannel<PAYLOAD>>() {

        @Override
        public StreamOutputChannel<PAYLOAD> getAttachment() {
          return StreamOutputChannel.this;
        }
      };

  /**
   * owner IPC pool.
   * */
  private final IPCConnectionPool ownerPool;

  /**
   * Channel release future.
   * */
  private ChannelFuture releaseFuture = null;

  /**
   * @param ecID exchange channel ID.StreamIOChannelID ecID
   * @param ownerPool the owner of this output channel.
   * @param initialPhysicalChannel the physical channel associated to this output channel in the beginning
   * */
  public StreamOutputChannel(final StreamIOChannelID ecID, final IPCConnectionPool ownerPool,
      final Channel initialPhysicalChannel) {
    super(ecID);
    outputDisableListeners = new ConcurrentLinkedQueue<IPCEventListener<StreamOutputChannel<PAYLOAD>>>();
    outputRecoverListeners = new ConcurrentLinkedQueue<IPCEventListener<StreamOutputChannel<PAYLOAD>>>();
    this.ownerPool = ownerPool;
    ChannelContext.getChannelContext(initialPhysicalChannel).getRegisteredChannelContext().getIOPair()
        .mapOutputChannel(this, initialPhysicalChannel);
  }

  /**
   * Call the method if the physical output device used by this {@link StreamOutputChannel} is not able to process
   * writes.
   * */
  public final void notifyOutputDisabled() {
    for (IPCEventListener<StreamOutputChannel<PAYLOAD>> l : outputDisableListeners) {
      l.triggered(this.outputDisabledEvent);
    }
  }

  /**
   * Call the method if the physical output device used by this {@link StreamOutputChannel} is able to process writes.
   * */
  public final void notifyOutputEnabled() {
    for (IPCEventListener<StreamOutputChannel<PAYLOAD>> l : outputRecoverListeners) {
      l.triggered(this.outputEnabledEvent);
    }
  }

  @Override
  public final String toString() {
    return "StreamOutputChannel{ ID: " + getID() + ",IOChannel: " + getIOChannel() + " }";
  }

  /**
   * @param t event type.
   * @param l event listener.
   * */
  public final void addListener(final IPCEvent<?> t, final IPCEventListener<StreamOutputChannel<PAYLOAD>> l) {
    if (t == this.outputDisabledEvent) {
      outputDisableListeners.add(l);
    } else if (t == this.outputEnabledEvent) {
      outputRecoverListeners.add(l);
    } else {
      throw new IllegalArgumentException("Unsupported event: " + t);
    }
  }

  /**
   * @param message the message to write
   * @return the write future.
   * */
  public final ChannelFuture write(final Object message) {
    Channel ch = getIOChannel();
    if (ch != null) {
      return ch.write(message);
    } else {
      throw new IllegalStateException("No usable physical IO channel.");
    }
  }

  /**
   * @return release future.
   * */
  public final synchronized ChannelFuture release() {
    if (releaseFuture == null) {
      releaseFuture = ownerPool.releaseLongTermConnection(this);
    }
    return releaseFuture;
  }

  /**
   * @return If the output channel is writable.
   * */
  public final boolean isWritable() {
    Channel ch = getIOChannel();
    return ch != null && ch.isWritable();
  }

}

package edu.washington.escience.myriad.parallel.ipc;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import edu.washington.escience.myriad.parallel.Producer;
import edu.washington.escience.myriad.parallel.ipc.IPCEvent.EventType;

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
  private final ConcurrentLinkedQueue<IPCEventListener> outputDisableListeners;

  /**
   * Output recovered listeners.
   * */
  private final ConcurrentLinkedQueue<IPCEventListener> outputRecoverListeners;

  /**
   * owner IPC pool.
   * */
  private final IPCConnectionPool ownerPool;

  /**
   * Output disabled event.
   * */
  public static final EventType OUTPUT_DISABLED = new EventType("Output disabled");

  /**
   * Output recovered event.
   * */
  public static final EventType OUTPUT_RECOVERED = new EventType("Output recovered");

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
    outputDisableListeners = new ConcurrentLinkedQueue<IPCEventListener>();
    outputRecoverListeners = new ConcurrentLinkedQueue<IPCEventListener>();
    this.ownerPool = ownerPool;
    ChannelContext.getChannelContext(initialPhysicalChannel).getRegisteredChannelContext().getIOPair()
        .mapOutputChannel(this);
  }

  /**
   * Call the method if the physical output device used by this {@link StreamOutputChannel} is not able to process
   * writes.
   * */
  final void notifyOutputDisabled() {
    for (IPCEventListener l : outputDisableListeners) {
      l.triggered(this.outputDisabledEvent);
    }
  }

  /**
   * Call the method if the physical output device used by this {@link StreamOutputChannel} is able to process writes.
   * */
  final void notifyOutputEnabled() {
    for (IPCEventListener l : outputRecoverListeners) {
      l.triggered(this.outputRecoveredEvent);
    }
  }

  @Override
  public final String toString() {
    return "StreamOutputChannel{ ID: " + getID() + ",IOChannel: " + getIOChannel() + " }";
  }

  /**
   * The output disabled event.
   * */
  private final IPCEvent outputDisabledEvent = new IPCEvent() {

    @Override
    public Object getAttachment() {
      return StreamOutputChannel.this;
    }

    @Override
    public EventType getType() {
      return OUTPUT_DISABLED;
    }

  };

  /**
   * The output recover event.
   * */
  private final IPCEvent outputRecoveredEvent = new IPCEvent() {

    @Override
    public Object getAttachment() {
      return StreamOutputChannel.this;
    }

    @Override
    public EventType getType() {
      return OUTPUT_RECOVERED;
    }

  };

  /**
   * @param t event type.
   * @param l event listener.
   * */
  public final void addListener(final EventType t, final IPCEventListener l) {
    if (t == OUTPUT_DISABLED) {
      outputDisableListeners.add(l);
    } else if (t == OUTPUT_RECOVERED) {
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

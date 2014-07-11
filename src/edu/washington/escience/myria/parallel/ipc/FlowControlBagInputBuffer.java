package edu.washington.escience.myria.parallel.ipc;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroupFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.parallel.ipc.IPCEvent.EventType;
import edu.washington.escience.myria.util.IPCUtils;
import edu.washington.escience.myria.util.concurrent.OrderedExecutorService;
import edu.washington.escience.myria.util.concurrent.ReentrantSpinLock;
import edu.washington.escience.myria.util.concurrent.ThreadStackDump;

/**
 * An flow control aware InputBuffer implementation. This type of InputBuffer has a soft capacity. The number of
 * messages held in this InputBuffer can be as large as {@link Integer.MAX_VALUE}. But the soft capacity is a trigger.<br>
 * If the soft capacity is meet, an IOEvent representing the buffer full event is triggered. <br>
 * If the
 * 
 * @param <PAYLOAD> the type of application defined data the input buffer is going to hold.
 * */
public final class FlowControlBagInputBuffer<PAYLOAD> extends BagInputBufferAdapter<PAYLOAD> {

  /**
   * logger.
   * */
  static final Logger LOGGER = LoggerFactory.getLogger(FlowControlBagInputBuffer.class);

  /**
   * After input buffer becomes full, if the size of the input buffer reaches this number, an input buffer recover event
   * is triggered.
   * */
  private final int recoverEventTrigger;

  /**
   * soft capacity, if the capacity is meet, a capacity full event is triggered, but the message will still be pushed
   * into the inner inputbuffer. It's up to the caller applications to respond to the capacity full event.
   * */
  private final int softCapacity;

  /**
   * serialize the buffer state events (FULL, EMPTY, RECOVER).
   * */
  private final ReentrantSpinLock bufferStateEventSerializeLock = new ReentrantSpinLock();

  /**
   * serialize the events.
   * */
  private final ReentrantSpinLock newInputSerializeLock = new ReentrantSpinLock();

  /**
   * Buffer state event. Input buffer full.
   * */
  public static final EventType INPUT_BUFFER_FULL = new EventType("Input buffer full");
  /**
   * Buffer state event. Input buffer empty.
   * */
  public static final EventType INPUT_BUFFER_EMPTY = new EventType("Input buffer empty");
  /**
   * Buffer state event. Input buffer recovered.
   * */
  public static final EventType INPUT_BUFFER_RECOVER = new EventType("Input buffer recovered");

  /**
   * new input data.
   * */
  public static final EventType NEW_INPUT_DATA = new EventType("new input data");

  /**
   * {@inheritDoc}.
   * 
   * @param softCapacity soft upper bound of the buffer size.
   * 
   * */
  public FlowControlBagInputBuffer(final IPCConnectionPool owner,
      final ImmutableSet<StreamIOChannelID> remoteChannelIDs, final int softCapacity, final int recoverEventTrigger) {
    super(owner, remoteChannelIDs);
    bufferEmptyListeners = new ConcurrentLinkedQueue<IPCEventListener>();
    bufferFullListeners = new ConcurrentLinkedQueue<IPCEventListener>();
    bufferRecoverListeners = new ConcurrentLinkedQueue<IPCEventListener>();
    newArrivalListeners = new ConcurrentLinkedQueue<IPCEventListener>();

    this.softCapacity = softCapacity;
    this.recoverEventTrigger = recoverEventTrigger;
  }

  @Override
  public String toString() {
    StringBuilder toStringBuilder = new StringBuilder();
    toStringBuilder.append(this.getClass().getSimpleName());
    toStringBuilder.append("[Processor: ");
    toStringBuilder.append(getProcessor());
    toStringBuilder.append("]");
    toStringBuilder.append("InputChannels: {\n");
    ImmutableSet<StreamIOChannelID> inputs = getSourceChannels();
    for (StreamIOChannelID id : inputs) {
      toStringBuilder.append("    ");
      toStringBuilder.append(getInputChannel(id));
      toStringBuilder.append("\n");
    }
    toStringBuilder.append("}");
    return toStringBuilder.toString();
  }

  @Override
  public void postStop() {
    this.resumeRead();
  }

  /**
   * Resume the read of all IO channels that are inputs of this input buffer.
   * 
   * @return ChannelGroupFuture denotes the future of the resume read action.
   * */
  public ChannelGroupFuture resumeRead() {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(this.toString());
    }

    LinkedList<ChannelFuture> allResumeFutures = new LinkedList<ChannelFuture>();
    ChannelGroup cg = new DefaultChannelGroup();
    for (final StreamIOChannelID inputID : getSourceChannels()) {
      Channel ch = getInputChannel(inputID).getIOChannel();
      if (ch != null && !ch.isReadable()) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Resume read for channel {}. Logical channel is {}", ch, inputID);
        }
        cg.add(ch);
        allResumeFutures.add(IPCUtils.resumeRead(ch));
      }
    }

    ChannelGroupFuture cgf = new DefaultChannelGroupFuture(cg, allResumeFutures);

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Finish resume for Stream {}", getProcessor());
    }
    return cgf;
  }

  /**
   * 
   * Pause read of all IO channels which are inputs of the @{link Consumer} operator with ID operatorID.
   * 
   * Called by Netty Upstream IO worker threads after pushing a data into an InputBuffer which has only a single empty
   * slot or already full.
   * 
   * @return ChannelGroupFuture denotes the future of the pause read action.
   * */
  public ChannelGroupFuture pauseRead() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(this.toString());
    }

    LinkedList<ChannelFuture> allPauseFutures = new LinkedList<ChannelFuture>();
    ChannelGroup cg = new DefaultChannelGroup();
    for (final StreamIOChannelID inputID : getSourceChannels()) {
      Channel ch = getInputChannel(inputID).getIOChannel();
      if (ch != null && ch.isReadable()) {
        allPauseFutures.add(IPCUtils.pauseRead(ch));
        cg.add(ch);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Pause read for channel {}, Logical channel is {}", ch, inputID);
        }
      }
    }
    return new DefaultChannelGroupFuture(cg, allPauseFutures);
  }

  @Override
  public void preStart(final Object processor) {
    if (isAttached()) {
      throw new IllegalStateException("Already attached to a processor: " + processor);
    }

    addListener(INPUT_BUFFER_FULL, new IPCEventListener() {
      @SuppressWarnings("unchecked")
      @Override
      public void triggered(final IPCEvent e) {
        if (((FlowControlBagInputBuffer<PAYLOAD>) (e.getAttachment())).remainingCapacity() <= 0) {
          pauseRead().awaitUninterruptibly();
        }
      }
    });
    addListener(INPUT_BUFFER_RECOVER, new IPCEventListener() {
      @SuppressWarnings("unchecked")
      @Override
      public void triggered(final IPCEvent e) {
        if (((FlowControlBagInputBuffer<PAYLOAD>) (e.getAttachment())).remainingCapacity() > 0) {
          resumeRead().awaitUninterruptibly();
        }
      }
    });
    addListener(INPUT_BUFFER_EMPTY, new IPCEventListener() {
      @SuppressWarnings("unchecked")
      @Override
      public void triggered(final IPCEvent e) {
        if (((FlowControlBagInputBuffer<PAYLOAD>) (e.getAttachment())).remainingCapacity() > 0) {
          resumeRead().awaitUninterruptibly();
        }
      }
    });
  }

  /**
   * @return the soft capacity.
   * */
  public int getCapacity() {
    return softCapacity;
  }

  /**
   * @return the remaining capacity.
   * */
  public int remainingCapacity() {
    return softCapacity - size();
  }

  @Override
  public void postClear() {
    checkOutputBufferStateEvents();
  }

  @Override
  protected void postOffer(final IPCMessage.StreamData<PAYLOAD> e, final boolean isSucceed) {
    if (isSucceed) {
      fireNewInput();
      checkInputBufferStateEvents();
    }
  }

  /**
   * Check events triggered by data input methods, i.e. offer.
   * */
  private void checkInputBufferStateEvents() {
    bufferStateEventSerializeLock.lock();
    try {
      if (remainingCapacity() <= 0 && previousEvent != INPUT_BUFFER_FULL) {
        fireBufferFull();
      }
    } finally {
      bufferStateEventSerializeLock.unlock();
    }
  }

  /**
   * Check events triggered by data output methods, i.e. poll/take/clear.
   * */
  private void checkOutputBufferStateEvents() {
    bufferStateEventSerializeLock.lock();
    try {
      if (isEmpty() && previousEvent != INPUT_BUFFER_EMPTY) {
        fireBufferEmpty();
      } else if (previousEvent == INPUT_BUFFER_FULL && size() <= recoverEventTrigger) {
        fireBufferRecover();
      }
    } finally {
      bufferStateEventSerializeLock.unlock();
    }
  }

  @Override
  public void postPoll(final IPCMessage.StreamData<PAYLOAD> m) {
    if (m != null) {
      checkOutputBufferStateEvents();
    }
  }

  @Override
  protected void postTimeoutPoll(final long time, final TimeUnit unit, final IPCMessage.StreamData<PAYLOAD> m) {
    if (m != null) {
      checkOutputBufferStateEvents();
    }
  }

  @Override
  public void postTake(final IPCMessage.StreamData<PAYLOAD> m) {
    if (m != null) {
      checkOutputBufferStateEvents();
    }
  }

  /**
   * Buffer empty event listeners.
   * */
  private final ConcurrentLinkedQueue<IPCEventListener> bufferEmptyListeners;

  /**
   * Buffer full event listeners.
   * */
  private final ConcurrentLinkedQueue<IPCEventListener> bufferFullListeners;

  /**
   * Buffer recover event listeners.
   * */
  private final ConcurrentLinkedQueue<IPCEventListener> bufferRecoverListeners;

  /**
   * new data event listeners.
   * */
  private final ConcurrentLinkedQueue<IPCEventListener> newArrivalListeners;

  /**
   * the buffer empty event.
   * */
  private final IPCEvent bufferEmptyEvent = new IPCEvent() {

    @Override
    public Object getAttachment() {
      return FlowControlBagInputBuffer.this;
    }

    @Override
    public EventType getType() {
      return INPUT_BUFFER_EMPTY;
    }

  };

  /**
   * the buffer empty event.
   * */
  private final IPCEvent newInputEvent = new IPCEvent() {

    @Override
    public Object getAttachment() {
      return FlowControlBagInputBuffer.this;
    }

    @Override
    public EventType getType() {
      return NEW_INPUT_DATA;
    }

  };

  /**
   * the buffer full event.
   * */
  private final IPCEvent bufferFullEvent = new IPCEvent() {

    @Override
    public Object getAttachment() {
      return FlowControlBagInputBuffer.this;
    }

    @Override
    public EventType getType() {
      return INPUT_BUFFER_FULL;
    }
  };

  /**
   * the buffer recover event.
   * */
  private final IPCEvent bufferRecoverEvent = new IPCEvent() {

    @Override
    public Object getAttachment() {
      return FlowControlBagInputBuffer.this;
    }

    @Override
    public EventType getType() {
      return INPUT_BUFFER_RECOVER;
    }
  };

  /**
   * Fire a buffer empty event. All the buffer empty event listeners will be notified.
   * 
   * New input event listeners are executed by trigger threads.
   * */
  protected void fireNewInput() {
    this.newInputSerializeLock.lock();
    try {
      for (IPCEventListener l : newArrivalListeners) {
        l.triggered(newInputEvent);
      }
    } finally {
      this.newInputSerializeLock.unlock();
    }
  }

  /**
   * protected by the event serialize lock.
   * */
  private EventType previousEvent = INPUT_BUFFER_EMPTY;

  /**
   * Fire a buffer empty event. All the buffer empty event listeners will be notified.
   * 
   * Listeners are executed by dedicated event executors.
   * */
  protected void fireBufferEmpty() {
    previousEvent = INPUT_BUFFER_EMPTY;
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Input buffer empty triggered in " + this, new ThreadStackDump());
    }
    getOwnerConnectionPool().getIPCEventProcessor().execute(
        new OrderedExecutorService.KeyRunnable<StreamInputBuffer<PAYLOAD>>() {

          @Override
          public void run() {
            for (IPCEventListener l : bufferEmptyListeners) {
              l.triggered(bufferEmptyEvent);
            }
          }

          @Override
          public StreamInputBuffer<PAYLOAD> getKey() {
            return FlowControlBagInputBuffer.this;
          }
        });

  }

  /**
   * Fire a buffer full event. All the buffer full event listeners will be notified.
   * 
   * Listeners are executed by dedicated event executors.
   * */
  protected void fireBufferFull() {
    previousEvent = INPUT_BUFFER_FULL;
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Input buffer full triggered in " + this, new ThreadStackDump());
    }
    getOwnerConnectionPool().getIPCEventProcessor().execute(
        new OrderedExecutorService.KeyRunnable<StreamInputBuffer<PAYLOAD>>() {

          @Override
          public void run() {
            for (IPCEventListener l : bufferFullListeners) {
              l.triggered(bufferFullEvent);
            }
          }

          @Override
          public StreamInputBuffer<PAYLOAD> getKey() {
            return FlowControlBagInputBuffer.this;
          }
        });

  }

  /**
   * Fire a buffer recover event. All the buffer recover event listeners will be notified.
   * 
   * Listeners are executed by dedicated event executors.
   * 
   * */
  protected void fireBufferRecover() {
    previousEvent = INPUT_BUFFER_RECOVER;
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Input buffer recover triggered in " + this, new ThreadStackDump());
    }
    getOwnerConnectionPool().getIPCEventProcessor().execute(
        new OrderedExecutorService.KeyRunnable<StreamInputBuffer<PAYLOAD>>() {

          @Override
          public void run() {
            for (IPCEventListener l : bufferRecoverListeners) {
              l.triggered(bufferRecoverEvent);
            }
          }

          @Override
          public StreamInputBuffer<PAYLOAD> getKey() {
            return FlowControlBagInputBuffer.this;
          }
        });

  }

  @Override
  public void addListener(final EventType t, final IPCEventListener listener) {

    if (t == INPUT_BUFFER_EMPTY) {
      bufferEmptyListeners.add(listener);
    } else if (t == INPUT_BUFFER_FULL) {
      bufferFullListeners.add(listener);
    } else if (t == INPUT_BUFFER_RECOVER) {
      bufferRecoverListeners.add(listener);
    } else if (t == NEW_INPUT_DATA) {
      newArrivalListeners.add(listener);
    } else {
      throw new IllegalArgumentException("Unsupported event type: " + t);
    }
  }

}

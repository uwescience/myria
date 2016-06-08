package edu.washington.escience.myria.parallel.ipc;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.parallel.ipc.IPCEvent.EventType;
import edu.washington.escience.myria.util.concurrent.ReentrantSpinLock;

/**
 * A simple InputBuffer implementation using bag semantic. The number of data held in this InputBuffer can be as large
 * as {@link Integer.MAX_VALUE}.
 *
 * @param <PAYLOAD> the type of application defined data the input buffer is going to hold.
 * */
public class SimpleBagInputBuffer<PAYLOAD> extends BagInputBufferAdapter<PAYLOAD> {

  /**
   * new input data.
   * */
  public static final EventType NEW_INPUT_DATA = new EventType("new input data");

  /**
   * serialize the events.
   * */
  private final ReentrantSpinLock newInputSerializeLock = new ReentrantSpinLock();

  /**
   * the buffer empty event.
   * */
  private final IPCEvent newInputEvent =
      new IPCEvent() {

        @Override
        public Object getAttachment() {
          return SimpleBagInputBuffer.this;
        }

        @Override
        public EventType getType() {
          return NEW_INPUT_DATA;
        }
      };

  /**
   * Fire a new input event. All the new arrival event listeners will be notified.
   *
   * New input event listeners are executed by trigger threads.
   * */
  protected final void fireNewInput() {
    newInputSerializeLock.lock();
    try {
      for (IPCEventListener l : newArrivalListeners) {
        l.triggered(newInputEvent);
      }
    } finally {
      newInputSerializeLock.unlock();
    }
  }

  @Override
  protected final void postOffer(final IPCMessage.StreamData<PAYLOAD> e, final boolean isSucceed) {
    if (isSucceed) {
      fireNewInput();
    }
  }

  /**
   * new data event listeners.
   * */
  private final ConcurrentLinkedQueue<IPCEventListener> newArrivalListeners;

  /**
   * logger.
   * */
  static final Logger LOGGER = LoggerFactory.getLogger(SimpleBagInputBuffer.class);

  /**
   * @param owner the owner IPC pool.
   * @param remoteChannelIDs from which channels, the data will input.
   * */
  public SimpleBagInputBuffer(
      final IPCConnectionPool owner, final ImmutableSet<StreamIOChannelID> remoteChannelIDs) {
    super(owner, remoteChannelIDs);
    newArrivalListeners = new ConcurrentLinkedQueue<IPCEventListener>();
  }

  /**
   * {@inheritDoc}.
   *
   * Only new data events is supported
   * */
  @Override
  public final void addListener(final EventType t, final IPCEventListener l) {
    if (t == NEW_INPUT_DATA) {
      newArrivalListeners.add(l);
    }
  }
}

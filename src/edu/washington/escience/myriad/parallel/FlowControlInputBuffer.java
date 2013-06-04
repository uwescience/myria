package edu.washington.escience.myriad.parallel;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.parallel.ipc.IPCEvent;
import edu.washington.escience.myriad.parallel.ipc.IPCEventListener;

/**
 * An flow control aware InputBuffer implementation. This type of InputBuffer has a soft capacity. The number of
 * messages held in this InputBuffer can be as large as {@link Integer.MAX_VALUE}. But the soft capacity is a trigger.<br>
 * If the soft capacity is meet, an IOEvent representing the buffer full event is triggered. <br>
 * If the
 * 
 * @param <M> the type of {@link ExchangeMessage} a FlowControlInputBuffer instance is going to hold.
 * */
public class FlowControlInputBuffer<M extends ExchangeMessage<TupleBatch>> implements InputBuffer<TupleBatch, M> {

  /**
   * the storage place of messages.
   * */
  private final LinkedList<M> storage;

  /**
   * the owner of this input buffer.
   * */
  private volatile ExchangePairID ownerOperator = null;

  /**
   * soft capacity, if the capacity is meet, a capacity full event is triggered, but the message will still be pushed
   * into the inner inputbuffer. It's up to the caller applications to respond to the capacity full event.
   * */
  private final int softCapacity;

  /**
   * serialize the events.
   * */
  private final Object eventSerializeLock = new Object();

  /**
   * @param softCapacity soft upper bound of the buffer size.
   * */
  public FlowControlInputBuffer(final int softCapacity) {
    this.storage = new LinkedList<M>();
    this.softCapacity = softCapacity;
    bufferEmptyListeners = new ConcurrentLinkedQueue<IPCEventListener<FlowControlInputBuffer<M>>>();
    bufferFullListeners = new ConcurrentLinkedQueue<IPCEventListener<FlowControlInputBuffer<M>>>();
    bufferRecoverListeners = new ConcurrentLinkedQueue<IPCEventListener<FlowControlInputBuffer<M>>>();
  }

  /**
   * @param ownerOperator the id of the owner operator.
   * 
   * */
  @Override
  public final void attach(final ExchangePairID ownerOperator) {
    this.ownerOperator = ownerOperator;
  }

  /**
   * @return the owner operator id.
   * */
  public final ExchangePairID getOwnerOperatorID() {
    return this.ownerOperator;
  }

  /**
   * @return the soft capacity.
   * */
  public final int getCapacity() {
    return this.softCapacity;
  }

  /**
   * @return the remaining capacity.
   * */
  public final int remainingCapacity() {
    synchronized (this.eventSerializeLock) {
      return this.softCapacity - this.storage.size();
    }
  }

  @Override
  public final int size() {
    synchronized (this.eventSerializeLock) {
      return this.storage.size();
    }
  }

  @Override
  public final boolean isEmpty() {
    synchronized (this.eventSerializeLock) {
      return this.storage.isEmpty();
    }
  }

  @Override
  public final void clear() {
    synchronized (this.eventSerializeLock) {
      this.storage.clear();
      fireBufferEmpty();
    }
  }

  @Override
  public final boolean offer(final M e) {
    if (this.ownerOperator == null) {
      return false;
    }
    synchronized (this.eventSerializeLock) {
      if (!this.storage.offer(e)) {
        return false;
      }
      if (this.remainingCapacity() <= 0) {
        this.fireBufferFull();
      }
      this.eventSerializeLock.notifyAll();
      return true;
    }
  }

  @Override
  public final M poll() {
    if (this.ownerOperator == null) {
      return null;
    }
    synchronized (this.eventSerializeLock) {
      M m = this.storage.poll();
      if (this.isEmpty()) {
        fireBufferEmpty();
      }

      if (m != null && this.remainingCapacity() == 1) {
        fireBufferRecover();
      }
      return m;
    }
  }

  @Override
  public final M poll(final long time, final TimeUnit unit) throws InterruptedException {
    if (this.ownerOperator == null) {
      return null;
    }
    synchronized (this.eventSerializeLock) {
      if (this.isEmpty()) {
        this.eventSerializeLock.wait(unit.toMillis(time));
      }
      M m = this.storage.poll();
      if (m == null) {
        return null;
      }
      if (this.isEmpty()) {
        fireBufferEmpty();
      } else if (this.remainingCapacity() == 1) {
        fireBufferRecover();
      }
      return m;
    }
  }

  @Override
  public final M take() throws InterruptedException {
    if (this.ownerOperator == null) {
      return null;
    }
    synchronized (this.eventSerializeLock) {
      while (this.isEmpty()) {
        this.eventSerializeLock.wait();
      }
      M m = this.storage.poll();
      if (m == null) {
        return null;
      }
      if (this.isEmpty()) {
        fireBufferEmpty();
      } else if (this.remainingCapacity() == 1) {
        fireBufferRecover();
      }
      return m;
    }
  }

  @Override
  public final M peek() {
    if (this.ownerOperator == null) {
      return null;
    }
    synchronized (this.eventSerializeLock) {
      return this.storage.peek();
    }
  }

  /**
   * Buffer empty event listeners.
   * */
  private final ConcurrentLinkedQueue<IPCEventListener<FlowControlInputBuffer<M>>> bufferEmptyListeners;

  /**
   * Buffer full event listeners.
   * */
  private final ConcurrentLinkedQueue<IPCEventListener<FlowControlInputBuffer<M>>> bufferFullListeners;

  /**
   * Buffer recover event listeners.
   * */
  private final ConcurrentLinkedQueue<IPCEventListener<FlowControlInputBuffer<M>>> bufferRecoverListeners;

  /**
   * the buffer empty event.
   * */
  private final IPCEvent<FlowControlInputBuffer<M>> bufferEmptyEvent = new IPCEvent<FlowControlInputBuffer<M>>() {

    @Override
    public final FlowControlInputBuffer<M> getAttachment() {
      return FlowControlInputBuffer.this;
    }
  };

  /**
   * the buffer full event.
   * */
  private final IPCEvent<FlowControlInputBuffer<M>> bufferFullEvent = new IPCEvent<FlowControlInputBuffer<M>>() {

    @Override
    public FlowControlInputBuffer<M> getAttachment() {
      return FlowControlInputBuffer.this;
    }
  };

  /**
   * the buffer recover event.
   * */
  private final IPCEvent<FlowControlInputBuffer<M>> bufferRecoverEvent = new IPCEvent<FlowControlInputBuffer<M>>() {

    @Override
    public FlowControlInputBuffer<M> getAttachment() {
      return FlowControlInputBuffer.this;
    }
  };

  /**
   * Add a buffer recover event listener.
   * 
   * @param e an IOEventListener.
   * */
  public final void addBufferEmptyListener(final IPCEventListener<FlowControlInputBuffer<M>> e) {
    if (this.ownerOperator != null) {
      if (this.isEmpty()) {
        e.triggered(bufferEmptyEvent);
      }
    }
    bufferEmptyListeners.add(e);
  }

  /**
   * Add a buffer recover event listener.
   * 
   * @param e an IOEventListener.
   * */
  public final void addBufferFullListener(final IPCEventListener<FlowControlInputBuffer<M>> e) {
    if (this.ownerOperator != null) {
      if (this.remainingCapacity() <= 0) {
        e.triggered(bufferFullEvent);
      }
    }
    bufferFullListeners.add(e);
  }

  /**
   * Add a buffer recover event listener.
   * 
   * @param e an IOEventListener.
   * */
  public final void addBufferRecoverListener(final IPCEventListener<FlowControlInputBuffer<M>> e) {
    bufferRecoverListeners.add(e);
  }

  /**
   * Fire a buffer empty event. All the buffer empty event listeners will be notified.
   * */
  private void fireBufferEmpty() {
    for (IPCEventListener<FlowControlInputBuffer<M>> l : bufferEmptyListeners) {
      l.triggered(bufferEmptyEvent);
    }
  }

  /**
   * Fire a buffer full event. All the buffer full event listeners will be notified.
   * */
  private void fireBufferFull() {
    for (IPCEventListener<FlowControlInputBuffer<M>> l : bufferFullListeners) {
      l.triggered(bufferFullEvent);
    }
  }

  /**
   * Fire a buffer recover event. All the buffer recover event listeners will be notified.
   * */
  private void fireBufferRecover() {
    for (IPCEventListener<FlowControlInputBuffer<M>> l : bufferRecoverListeners) {
      l.triggered(bufferRecoverEvent);
    }
  }

  @Override
  public final M pickFirst(final int sourceID) {
    if (this.ownerOperator == null) {
      return null;
    }
    Iterator<M> it = this.storage.iterator();
    while (it.hasNext()) {
      M ed = it.next();
      if (sourceID == ed.getSourceIPCID()) {
        it.remove();
        return ed;
      }
    }
    return null;
  }

  @Override
  public final void detached() {
    if (this.ownerOperator != null) {
      this.ownerOperator = null;
      this.clear();
    }
  }
}

package edu.washington.escience.myria.parallel.ipc;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.parallel.ipc.IPCMessage.StreamData;
import edu.washington.escience.myria.util.AttachmentableAdapter;
import edu.washington.escience.myria.util.concurrent.ClosableReentrantLock;

/**
 * A simple InputBuffer implementation. The number of messages held in this InputBuffer can be as large as
 * {@link Integer.MAX_VALUE}. All the input data from different input channels are treated by bag semantic. No order is
 * is guaranteed.
 *
 * @param <PAYLOAD> the type of application defined data the input buffer is going to hold.
 * */
public abstract class BagInputBufferAdapter<PAYLOAD> extends AttachmentableAdapter
    implements StreamInputBuffer<PAYLOAD> {

  /**
   * logger.
   * */
  static final Logger LOGGER = LoggerFactory.getLogger(BagInputBufferAdapter.class);

  /**
   * input channel state.
   * */
  private class InputChannelState {
    /**
     * EOS bit.
     * */
    private final AtomicBoolean eos = new AtomicBoolean(false);
    /**
     * EOS lock.
     * */
    private final ReadWriteLock eosLock = new ReentrantReadWriteLock();
    /**
     * input channel.
     * */
    private final StreamInputChannel<PAYLOAD> inputChannel;

    /**
     * @param id input channel id.
     * */
    InputChannelState(final StreamIOChannelID id) {
      inputChannel = new StreamInputChannel<PAYLOAD>(id, BagInputBufferAdapter.this);
    }
  }

  /**
   * the storage place of messages.
   * */
  private final LinkedBlockingQueue<IPCMessage.StreamData<PAYLOAD>> storage;

  /**
   * Num of EOS.
   * */
  private int numInputEOS;

  /**
   * Num threads waiting data by poll(timeout) or take.
   * */
  private int numWaiting;

  /**
   * Set of input channels.
   * */
  private final ImmutableMap<StreamIOChannelID, InputChannelState> inputChannels;

  /**
   * The processor attached.
   * */
  private final AtomicReference<Object> processor;

  /**
   * owner.
   * */
  private final IPCConnectionPool ownerConnectionPool;

  /**
   * Input buffer size.
   * */
  private int size = 0;

  /**
   * Serialize buffer size access.
   * */
  private final ClosableReentrantLock bufferSizeLock = new ClosableReentrantLock();

  /**
   * @return buffer size lock.
   * */
  public final ClosableReentrantLock getBufferSizeLock() {
    return bufferSizeLock;
  }

  /**
   * wait on empty.
   * */
  private final Condition emptySize = bufferSizeLock.newCondition();

  /**
   * @param owner the owner IPC pool.
   * @param remoteChannelIDs from which channels, the data will input.
   * */
  public BagInputBufferAdapter(
      final IPCConnectionPool owner, final ImmutableSet<StreamIOChannelID> remoteChannelIDs) {
    storage = new LinkedBlockingQueue<IPCMessage.StreamData<PAYLOAD>>();
    ImmutableMap.Builder<StreamIOChannelID, InputChannelState> b = ImmutableMap.builder();
    for (StreamIOChannelID ecID : remoteChannelIDs) {
      InputChannelState ics = new InputChannelState(ecID);
      b.put(ecID, ics);
    }
    inputChannels = b.build();
    processor = new AtomicReference<Object>();
    numInputEOS = 0;
    numWaiting = 0;
    ownerConnectionPool = owner;
  }

  /**
   * Called before {@link #start(Object)} operations are conducted.
   *
   * @param processor {@link #start(Object)}
   * @throws IllegalStateException if the {@link #start(Object)} operation should not be done.
   * */
  protected void preStart(final Object processor) throws IllegalStateException {}

  /**
   * Called after {@link #start(Object)} operations are conducted.
   *
   * @param processor {@link #start(Object)}
   * */
  protected void postStart(final Object processor) {}

  @Override
  public final void start(final Object processor) {
    Preconditions.checkNotNull(processor);
    preStart(processor);
    if (!this.processor.compareAndSet(null, processor)) {
      throw new IllegalStateException("Already attached to a processor: " + processor);
    }
    this.getOwnerConnectionPool().registerStreamInput(this);
    postStart(processor);
  }

  /**
   * Called before {@link #stop()} operations are conducted.
   *
   * @throws IllegalStateException if the {@link #stop()} operation should not be done.
   * */
  protected void preStop() throws IllegalStateException {}

  /**
   * Called after {@link #stop()} operations are conducted.
   *
   * */
  protected void postStop() {}

  @Override
  public final void stop() {
    this.preStop();
    this.processor.set(null);
    this.clear();
    this.postStop();
  }

  @Override
  public final int size() {
    try (ClosableReentrantLock l = bufferSizeLock.open()) {
      return this.size;
    }
  }

  @Override
  public final boolean isEmpty() {
    try (ClosableReentrantLock l = bufferSizeLock.open()) {
      return this.size == 0;
    }
  }

  /**
   * Called before {@link #clear()} is executed.
   *
   * @throws IllegalStateException if the {@link #clear()} operation should not be done.
   * */
  protected void preClear() throws IllegalStateException {}

  /**
   * Called after {@link #clear()} is executed.
   * */
  protected void postClear() {}

  @Override
  public final void clear() {
    preClear();
    storage.clear();
    postClear();
    try (ClosableReentrantLock l = bufferSizeLock.open()) {
      this.size = 0;
    }
  }

  /**
   * Check if the input buffer is attached.
   * */
  private void checkAttached() {
    if (!isAttached()) {
      throw new IllegalStateException("Not attached");
    }
  }

  /**
   * @param e input data.
   * @return input state of the data coming channel.
   * */
  private InputChannelState checkValidInputChannel(final IPCMessage.StreamData<PAYLOAD> e) {
    StreamIOChannelID sourceId = new StreamIOChannelID(e.getStreamID(), e.getRemoteID());
    InputChannelState s = inputChannels.get(sourceId);
    if (s == null) {
      throw new IllegalArgumentException("Message received from unknown input channel" + e);
    }

    return s;
  }

  /**
   * @param ics input channel state
   * @param e the input data.
   * @return false if it's an EOS, true if it's not.
   * */
  private boolean checkNotEOS(final InputChannelState ics, final IPCMessage.StreamData<PAYLOAD> e) {
    if (ics.eos.get()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Message received from an already EOS channele from remote "
                + e.getRemoteID()
                + " streamID "
                + e.getStreamID()
                + " "
                + ics.inputChannel);
      }
      /* temp solution, better to check if it's from a recover worker */
      return false;
    }
    return true;
  }

  /**
   * Called before {@link #offer(StreamData)} operations are conducted.
   *
   * @param msg {@link #offer(edu.washington.escience.myria.parallel.ipc.IPCMessage.StreamData)}
   * @throws IllegalStateException if the
   *           {@link #offer(edu.washington.escience.myria.parallel.ipc.IPCMessage.StreamData)} operation should not be
   *           done.
   * */
  protected void preOffer(final IPCMessage.StreamData<PAYLOAD> msg) throws IllegalStateException {}

  /**
   * Called after {@link #offer(StreamData)} operations are conducted.
   *
   * @param msg {@link #offer(StreamData)}
   * @param isSucceed if the offer operation succeeds
   * */
  protected void postOffer(final IPCMessage.StreamData<PAYLOAD> msg, final boolean isSucceed) {}

  @Override
  public final boolean offer(final IPCMessage.StreamData<PAYLOAD> msg) {
    Preconditions.checkNotNull(msg);
    checkAttached();
    InputChannelState ics = checkValidInputChannel(msg);
    if (!checkNotEOS(ics, msg)) {
      return true;
    }
    preOffer(msg);

    if (msg.getPayload() == null) { // EOS msg
      ics.eosLock.writeLock().lock();
      checkNotEOS(ics, msg);
      ics.eos.set(true);
    } else {
      ics.eosLock.readLock().lock();
      checkNotEOS(ics, msg);
    }

    boolean inserted = false;
    try {

      inserted = storage.offer(msg);
      if (inserted) {
        try (ClosableReentrantLock l = bufferSizeLock.open()) {
          if (msg.getPayload() == null) {
            this.numInputEOS += 1;
          }
          this.size += 1;
          if (numWaiting > 0) {
            if (isEOS()) {
              emptySize.signalAll();
            } else if (!isEmpty()) {
              emptySize.signal();
            }
          }
        }
      }
    } finally {
      if (msg.getPayload() == null) {
        ics.eosLock.writeLock().unlock();
      } else {
        ics.eosLock.readLock().unlock();
      }
    }
    this.postOffer(msg, inserted);

    return inserted;
  }

  /**
   * Called before {@link #take()} operations are conducted.
   *
   * @throws IllegalStateException if the {@link #take()} operation should not be done.
   * */
  protected void preTake() throws IllegalStateException {}

  /**
   * Called after {@link #take()} operations are conducted.
   *
   * @param msg the result of {@link #take()}.
   * */
  protected void postTake(final IPCMessage.StreamData<PAYLOAD> msg) {}

  @Override
  public final IPCMessage.StreamData<PAYLOAD> take() throws InterruptedException {
    checkAttached();

    if (isEOS() && isEmpty()) {
      return null;
    }
    preTake();

    try (ClosableReentrantLock l = bufferSizeLock.open()) {
      if (isEmpty() && !isEOS()) {
        numWaiting++;
        try {
          emptySize.await();
        } finally {
          numWaiting--;
        }
      }
      if (!isEmpty()) {
        size -= 1;
      } else {
        return null;
      }
    }
    IPCMessage.StreamData<PAYLOAD> m = storage.poll();
    postTake(m);

    return m;
  }

  /**
   * Called before {@link #poll(long, TimeUnit)} operations are conducted.
   *
   * @param time param of {@link #poll(long, TimeUnit)}
   * @param unit param of {@link #poll(long, TimeUnit)}
   * @throws IllegalStateException if the {@link #poll(long, TimeUnit)} operation should not be done.
   * */
  protected void preTimeoutPoll(final long time, final TimeUnit unit)
      throws IllegalStateException {}

  /**
   * Called after {@link #poll(long, TimeUnit)} operations are conducted.
   *
   * @param time param of {@link #poll(long, TimeUnit)}
   * @param unit param of {@link #poll(long, TimeUnit)}
   * @param msg the result of {@link #poll(long, TimeUnit)}.
   * */
  protected void postTimeoutPoll(
      final long time, final TimeUnit unit, final IPCMessage.StreamData<PAYLOAD> msg) {}

  @Override
  public final IPCMessage.StreamData<PAYLOAD> poll(final long time, final TimeUnit unit)
      throws InterruptedException {
    checkAttached();

    if (isEOS() && isEmpty()) {
      return null;
    }
    preTimeoutPoll(time, unit);

    try (ClosableReentrantLock l = bufferSizeLock.open()) {
      if (isEmpty() && !isEOS()) {
        numWaiting++;
        try {
          emptySize.await(time, unit);
        } finally {
          numWaiting--;
        }
        if (isEmpty()) {
          return null;
        }
      }
      this.size -= 1;
    }
    IPCMessage.StreamData<PAYLOAD> m = this.storage.poll();

    postTimeoutPoll(time, unit, m);

    return m;
  }

  /**
   * Called before {@link #poll()} operations are conducted.
   *
   * @throws IllegalStateException if the {@link #poll()} operation should not be done.
   * */
  protected void prePoll() throws IllegalStateException {}

  /**
   * Called after {@link #poll()} operations are conducted.
   *
   * @param msg the result of {@link #poll()}.
   * */
  protected void postPoll(final IPCMessage.StreamData<PAYLOAD> msg) {}

  @Override
  public final IPCMessage.StreamData<PAYLOAD> poll() {
    checkAttached();

    prePoll();
    try (ClosableReentrantLock l = bufferSizeLock.open()) {
      if (isEmpty()) {
        return null;
      }
      size -= 1;
    }
    IPCMessage.StreamData<PAYLOAD> m = storage.poll();
    postPoll(m);

    return m;
  }

  @Override
  public final IPCMessage.StreamData<PAYLOAD> peek() {
    checkAttached();

    return storage.peek();
  }

  @Override
  public final boolean isAttached() {
    return processor.get() != null;
  }

  @Override
  public final boolean isEOS() {
    try (ClosableReentrantLock l = bufferSizeLock.open()) {
      return numInputEOS >= inputChannels.size();
    }
  }

  @Override
  public final StreamInputChannel<PAYLOAD> getInputChannel(
      final StreamIOChannelID sourceChannelID) {
    return inputChannels.get(sourceChannelID).inputChannel;
  }

  @Override
  public final ImmutableSet<StreamIOChannelID> getSourceChannels() {
    return inputChannels.keySet();
  }

  @Override
  public final IPCConnectionPool getOwnerConnectionPool() {
    return ownerConnectionPool;
  }

  @Override
  public final Object getProcessor() {
    return processor.get();
  }
}

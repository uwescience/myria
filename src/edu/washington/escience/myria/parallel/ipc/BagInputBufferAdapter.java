package edu.washington.escience.myria.parallel.ipc;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.parallel.ipc.IPCMessage.StreamData;
import edu.washington.escience.myria.util.AttachmentableAdapter;

/**
 * A simple InputBuffer implementation. The number of messages held in this InputBuffer can be as large as
 * {@link Integer.MAX_VALUE}. All the input data from different input channels are treated by bag semantic. No order is
 * is guaranteed.
 * 
 * @param <PAYLOAD> the type of application defined data the input buffer is going to hold.
 * */
public abstract class BagInputBufferAdapter<PAYLOAD> extends AttachmentableAdapter implements
    StreamInputBuffer<PAYLOAD> {

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
  private final AtomicInteger numEOS;

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
   * wait on empty.
   * */
  private final Object emptyWaitingLock = new Object();

  /**
   * owner.
   * */
  private final IPCConnectionPool ownerConnectionPool;

  /**
   * @param owner the owner IPC pool.
   * @param remoteChannelIDs from which channels, the data will input.
   * */
  public BagInputBufferAdapter(final IPCConnectionPool owner, final ImmutableSet<StreamIOChannelID> remoteChannelIDs) {
    storage = new LinkedBlockingQueue<IPCMessage.StreamData<PAYLOAD>>();
    ImmutableMap.Builder<StreamIOChannelID, InputChannelState> b = ImmutableMap.builder();
    for (StreamIOChannelID ecID : remoteChannelIDs) {
      InputChannelState ics = new InputChannelState(ecID);
      b.put(ecID, ics);
    }
    inputChannels = b.build();
    processor = new AtomicReference<Object>();
    numEOS = new AtomicInteger(0);
    numWaiting = 0;
    ownerConnectionPool = owner;
  }

  /**
   * Called before {@link #start(Object)} operations are conducted.
   * 
   * @param processor {@link #start(Object)}
   * @throws IllegalStateException if the {@link #start(Object)} operation should not be done.
   * */
  protected void preStart(final Object processor) throws IllegalStateException {
  }

  /**
   * Called after {@link #start(Object)} operations are conducted.
   * 
   * @param processor {@link #start(Object)}
   * */
  protected void postStart(final Object processor) {

  }

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

  @Override
  public final int size() {
    return storage.size();
  }

  @Override
  public final boolean isEmpty() {
    return storage.isEmpty();
  }

  /**
   * Called before {@link #clear()} is executed.
   * 
   * @throws IllegalStateException if the {@link #clear()} operation should not be done.
   * */
  protected void preClear() throws IllegalStateException {
  }

  /**
   * Called after {@link #clear()} is executed.
   * */
  protected void postClear() {
  }

  @Override
  public final void clear() {
    preClear();
    storage.clear();
    postClear();
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
        LOGGER.debug("Message received from an already EOS channele from remote " + e.getRemoteID() + " streamID "
            + e.getStreamID() + " " + ics.inputChannel);
      }
      /* temp solution, better to check if it's from a recover worker */
      return false;
      // throw new IllegalStateException("Message received from an already EOS channele: " + e);
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
  protected void preOffer(final IPCMessage.StreamData<PAYLOAD> msg) throws IllegalStateException {
  }

  /**
   * Called after {@link #offer(StreamData)} operations are conducted.
   * 
   * @param msg {@link #offer(StreamData)}
   * @param isSucceed if the offer operation succeeds
   * */
  protected void postOffer(final IPCMessage.StreamData<PAYLOAD> msg, final boolean isSucceed) {
  }

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
      numEOS.incrementAndGet();
    } else {
      ics.eosLock.readLock().lock();
      checkNotEOS(ics, msg);
    }

    boolean inserted = false;
    try {

      inserted = storage.offer(msg);
      if (inserted) {
        synchronized (emptyWaitingLock) {
          if (numWaiting > 0) {
            if (isEOS()) {
              emptyWaitingLock.notifyAll();
            } else if (!isEmpty()) {
              emptyWaitingLock.notify();
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
  protected void preTake() throws IllegalStateException {
  }

  /**
   * Called after {@link #take()} operations are conducted.
   * 
   * @param msg the result of {@link #take()}.
   * */
  protected void postTake(final IPCMessage.StreamData<PAYLOAD> msg) {
  }

  @Override
  public final IPCMessage.StreamData<PAYLOAD> take() throws InterruptedException {
    if (!isAttached()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Input buffer not attached.");
      }
      return null;
    }

    if (isEOS() && isEmpty()) {
      return null;
    }
    preTake();

    IPCMessage.StreamData<PAYLOAD> m = storage.poll();
    while (m == null) {
      if (isEOS()) {
        return null;
      }
      synchronized (emptyWaitingLock) {
        if (isEmpty() && !isEOS()) {
          numWaiting++;
          try {
            emptyWaitingLock.wait();
          } finally {
            numWaiting--;
          }
        }
      }
      m = storage.poll();
    }
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
  protected void preTimeoutPoll(final long time, final TimeUnit unit) throws IllegalStateException {
  }

  /**
   * Called after {@link #poll(long, TimeUnit)} operations are conducted.
   * 
   * @param time param of {@link #poll(long, TimeUnit)}
   * @param unit param of {@link #poll(long, TimeUnit)}
   * @param msg the result of {@link #poll(long, TimeUnit)}.
   * */
  protected void postTimeoutPoll(final long time, final TimeUnit unit, final IPCMessage.StreamData<PAYLOAD> msg) {
  }

  @Override
  public final IPCMessage.StreamData<PAYLOAD> poll(final long time, final TimeUnit unit) throws InterruptedException {
    if (!isAttached()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Input buffer not attached.");
      }
      return null;
    }

    if (isEOS() && isEmpty()) {
      return null;
    }
    preTimeoutPoll(time, unit);
    long toWaitMilli = unit.toMillis(time);
    long startNano = System.nanoTime();

    IPCMessage.StreamData<PAYLOAD> m = storage.poll();
    while (m == null) {
      if (isEOS() && isEmpty()) {
        return null;
      }
      long wt = toWaitMilli - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);
      if (wt <= 0) {
        break;
      }
      synchronized (emptyWaitingLock) {
        if (isEmpty() && !isEOS()) {
          numWaiting++;
          try {
            emptyWaitingLock.wait(wt);
          } finally {
            numWaiting--;
          }
        }
      }
      m = storage.poll();
    }
    postTimeoutPoll(time, unit, m);
    return m;
  }

  /**
   * Called before {@link #poll()} operations are conducted.
   * 
   * @throws IllegalStateException if the {@link #poll()} operation should not be done.
   * */
  protected void prePoll() throws IllegalStateException {
  }

  /**
   * Called after {@link #poll()} operations are conducted.
   * 
   * @param msg the result of {@link #poll()}.
   * */
  protected void postPoll(final IPCMessage.StreamData<PAYLOAD> msg) {
  }

  @Override
  public final IPCMessage.StreamData<PAYLOAD> poll() {
    if (!isAttached()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Input buffer not attached.");
      }
      return null;
    }
    prePoll();
    IPCMessage.StreamData<PAYLOAD> m = storage.poll();
    postPoll(m);
    return m;
  }

  @Override
  public final IPCMessage.StreamData<PAYLOAD> peek() {
    if (!isAttached()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Input buffer not attached.");
      }
      return null;
    }
    return storage.peek();
  }

  @Override
  public final boolean isAttached() {
    return processor.get() != null;
  }

  @Override
  public final boolean isEOS() {
    return numEOS.get() >= inputChannels.size();
  }

  @Override
  public final StreamInputChannel<PAYLOAD> getInputChannel(final StreamIOChannelID sourceChannelID) {
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

package edu.washington.escience.myriad.parallel;

import java.util.concurrent.TimeUnit;

/**
 * The interface represents an input buffer for a {@link Consumer} operator. All the messages that can be put into the
 * {@link InputBuffer} should be sub class of {@link ExchangeMessage}.
 * 
 * @param <M> the actual message type
 * @param <DATA> the data type that held in the {@link ExchangeMessage}.
 * */
public interface InputBuffer<DATA, M extends ExchangeMessage<DATA>> {

  /**
   * @return current buffer size.
   * */
  int size();

  /**
   * @return if the buffer is empty.
   * */
  boolean isEmpty();

  /**
   * clear the buffer.
   * */
  void clear();

  /**
   * @param e the message.
   * @return true if the message gets successfully put into the input buffer
   * */
  boolean offer(final M e);

  /**
   * Retrieves and removes the head of this input buffer, or returns <tt>null</tt> if this input buffer is empty.
   * 
   * @return the head of this input buffer, or <tt>null</tt> if this input buffer is empty
   * @see java.util.Queue#poll()
   */
  M poll();

  /**
   * 
   * 
   * Retrieves and removes the head of this input buffer, waiting up to the specified wait time if necessary for an
   * element to become available.
   * 
   * @param timeout how long to wait before giving up, in units of <tt>unit</tt>
   * @param unit a <tt>TimeUnit</tt> determining how to interpret the <tt>timeout</tt> parameter
   * @return the head of this input buffer, or <tt>null</tt> if the specified waiting time elapses before an element is
   *         available
   * @throws InterruptedException if interrupted while waiting
   * @see java.util.concurrent.BlockingQueue#poll(long, TimeUnit)
   */
  M poll(long timeout, TimeUnit unit) throws InterruptedException;

  /**
   * Retrieves and removes the head of this input buffer, waiting if necessary until an element becomes available.
   * 
   * @return the head of this input buffer
   * @throws InterruptedException if interrupted while waiting
   * @see java.util.concurrent.BlockingQueue#take()
   */
  M take() throws InterruptedException;

  /**
   * Retrieves, but does not remove, the head of this input buffer, or returns <tt>null</tt> if this input buffer is
   * empty.
   * 
   * @return the head of this input buffer, or <tt>null</tt> if this input buffer is empty
   * @see java.util.Queue#peek();
   * */
  M peek();

  /**
   * Search from the head, get the first message from the sourceID.
   * 
   * @param sourceID the source workerID
   * @return the message.
   * */
  M pickFirst(int sourceID);

}

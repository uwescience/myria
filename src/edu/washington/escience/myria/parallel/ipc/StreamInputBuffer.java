package edu.washington.escience.myria.parallel.ipc;

import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.parallel.ipc.IPCEvent.EventType;
import edu.washington.escience.myria.util.Attachmentable;

/**
 * The interface represents an input buffer for a {@link Consumer} operator. All the messages that can be put into the
 * {@link StreamInputBuffer} should be sub class of {@link ExchangeMessage}.
 *
 * @param <PAYLOAD> the type of application defined data the input buffer is going to hold.
 * */
public interface StreamInputBuffer<PAYLOAD> extends Attachmentable {

  /**
   * @return the set of input channel IDs.
   * */
  ImmutableSet<StreamIOChannelID> getSourceChannels();

  /**
   * @return The number of messages in current buffer.
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
   * Add a new message to this input buffer. It is required that:
   * <ol>
   * <li>For messages from the same input channel (i.e. messages with the same (msg.remoteID, msg.streamID) pair), the
   * messages are required to get added sequentially</li>
   * </ol>
   *
   * @param msg the message.
   * @return true if the message gets successfully put into the input buffer
   * @throws NullPointerException if the msg is null
   * @throws IllegalArgumentException if the msg is from a channel which is not the input channel of this input buffer.
   * @throws IllegalStateException if the input buffer is not attached or if the source channel is already EOS.
   * */
  boolean offer(final IPCMessage.StreamData<PAYLOAD> msg)
      throws NullPointerException, IllegalArgumentException, IllegalStateException;

  /**
   * Retrieves and removes the head of this input buffer, or returns <tt>null</tt> if no message can be retrieved at
   * this moment.
   *
   * @return the head of this input buffer, or <tt>null</tt> if no output message can be retrieved.
   */
  IPCMessage.StreamData<PAYLOAD> poll();

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
   */
  IPCMessage.StreamData<PAYLOAD> poll(long timeout, TimeUnit unit) throws InterruptedException;

  /**
   * Retrieves and removes the head of this input buffer, waiting if necessary until an element becomes available.
   *
   * Return null if isEmpty() and isEOS()
   *
   * @return the head of this input buffer
   * @throws InterruptedException if interrupted while waiting
   */
  IPCMessage.StreamData<PAYLOAD> take() throws InterruptedException;

  /**
   * Retrieves, but does not remove, the head of this input buffer, or returns <tt>null</tt> if no message can be
   * retrieved.
   *
   * @return the head of this input buffer, or <tt>null</tt> if no message can be retrieved at this moment.
   * */
  IPCMessage.StreamData<PAYLOAD> peek();

  /**
   * Attach the input buffer to a processor, and also register itself in the owner {@link IPCConnectionPool}. An input
   * buffer must be attached before any message can be buffered. Messages put into the input buffer before it is
   * attached will be dropped directly. Once the input buffer is attached, it is never detached.
   *
   * @param processorIdentifier the identifier of the input buffer processor. It can be any {@link Object} but non-null.
   *          The object is not used inside the input buffer.
   * */
  void start(final Object processorIdentifier);

  /**
   * Stop this input buffer. Clean up.
   * */
  void stop();

  /**
   * @return if the input buffer is attached.
   * */
  boolean isAttached();

  /**
   * This method should return true once all the input streams have EOS. It only means no new input messages will be
   * added into the InputBuffer, but the InputBuffer may not be empty.
   *
   * @return if all the message streams have ended.
   * */
  boolean isEOS();

  /**
   * @param t event type
   * @param l event listener
   * */
  void addListener(final EventType t, final IPCEventListener l);

  /**
   * @param sourceChannelID source channel ID
   * @return input channel.
   * */
  StreamInputChannel<PAYLOAD> getInputChannel(final StreamIOChannelID sourceChannelID);

  /**
   * @return owner connection pool.
   * */
  IPCConnectionPool getOwnerConnectionPool();

  /**
   * @return application defined message processor.
   * */
  Object getProcessor();
}

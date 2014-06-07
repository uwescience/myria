package edu.washington.escience.myria.parallel.ipc;

import edu.washington.escience.myria.operator.network.Consumer;

/**
 * 
 * An {@link StreamInputChannel} represents a partition of a {@link Consumer} input .
 * 
 * @param <PAYLOAD> the type of payload that this input channel will receive.
 * */
public class StreamInputChannel<PAYLOAD> extends StreamIOChannel {

  /** The logger for this class. */
  static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(StreamInputChannel.class.getName());

  /**
   * The input buffer into which the messages from this channel should be pushed.
   * */
  private final StreamInputBuffer<PAYLOAD> inputBuffer;

  /**
   * @param ib the destination input buffer.
   * @param ecID exchange channel ID.
   * */
  StreamInputChannel(final StreamIOChannelID ecID, final StreamInputBuffer<PAYLOAD> ib) {
    super(ecID);
    inputBuffer = ib;
  }

  @Override
  public final String toString() {
    return "StreamInput{ ID: " + getID() + ", IOChannel: " + getIOChannel() + " }";
  }

  /**
   * @return the destination input buffer.
   * */
  final StreamInputBuffer<PAYLOAD> getInputBuffer() {
    return inputBuffer;
  }
}

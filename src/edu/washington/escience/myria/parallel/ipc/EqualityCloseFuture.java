package edu.washington.escience.myria.parallel.ipc;

import org.jboss.netty.channel.Channel;

/**
 * A condition close future that is conditioned on the equality of the expected object and the actual object.
 *
 * @param <T> type.
 * */
public class EqualityCloseFuture<T> extends ConditionCloseFuture {

  /**
   * expected value.
   * */
  private final T expected;

  /**
   * @param channel the channel who owns the future
   * @param expected the expected value
   * */
  EqualityCloseFuture(final Channel channel, final T expected) {
    super(channel);
    this.expected = expected;
  }

  /**
   * Set the actual value.
   *
   * @param actual the actual value
   * */
  public final void setActual(final T actual) {
    if (expected == null) {
      setCondition(actual == null);
    } else {
      setCondition(expected.equals(actual));
    }
  }
}

package edu.washington.escience.myriad.parallel.ipc;

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

  EqualityCloseFuture(final Channel channel, final T expected) {
    super(channel);
    this.expected = expected;
  }

  public void setActual(final T actual) {
    if (expected == null) {
      setCondition(actual == null);
    } else {
      setCondition(expected.equals(actual));
    }
  }

}

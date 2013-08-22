package edu.washington.escience.myria.parallel;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.operator.Operator;

/**
 * The producer part of broadcast operator
 * 
 * BroadcastProducer send multiple copies of tuples to a set of workers. Each worker will receive the same tuples.
 * 
 * @author Shumo Chu (chushumo@cs.washington.edu)
 * @deprecated
 */
@Deprecated
public class BroadcastProducer extends Producer {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * @param child the child that feed data to this producer
   * @param operatorID the destination operator where the data goes
   * @param workerIDs the set of work IDs that receive the broadcast data
   */
  public BroadcastProducer(final Operator child, final ExchangePairID operatorID, final int[] workerIDs) {
    super(child, operatorID, workerIDs);
  }

  @Override
  protected final void consumeTuples(final TupleBatch tuples) throws DbException {

    TupleBatch dm = null;
    tuples.compactInto(getBuffers()[0]);

    while ((dm = getBuffers()[0].popAnyUsingTimeout()) != null) {

      /* broadcast message to multiple workers */
      for (int i = 0; i < numChannels(); i++) {
        try {
          writeMessage(i, dm);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  @Override
  protected final void childEOS() throws DbException {

    TupleBatch dm = null;
    int numChannels = super.numChannels();

    System.out.println("num channels:" + numChannels());
    System.out.println("buffer length:" + getBuffers().length);

    /* BroadcastProducer only uses getBuffers()[0] */
    while ((dm = getBuffers()[0].popAny()) != null) {
      for (int i = 0; i < numChannels(); i++) {
        try {
          writeMessage(i, dm);
          System.out.println("i :" + i);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }

    for (int i = 0; i < numChannels; i++) {
      super.channelEnds(i);
    }
  }

  @Override
  protected final void childEOI() throws DbException {

    TupleBatch dm = null;
    TupleBatch.eoiTupleBatch(getSchema()).compactInto(getBuffers()[0]);

    /* BroadcastProducer only uses getBuffers()[0] */
    while ((dm = getBuffers()[0].popAny()) != null) {
      for (int i = 0; i < numChannels(); i++) {
        try {
          writeMessage(i, dm);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }
}

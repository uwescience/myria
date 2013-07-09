package edu.washington.escience.myriad.parallel;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.util.IPCUtils;

/**
 * The producer part of broadcast operator
 * 
 * BroadcastProducer send multiple copies of tuples to a set of workers. Each worker will receive the same tuples.
 * 
 * @author Shumo Chu (chushumo@cs.washington.edu)
 * 
 */
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

    TransportMessage dm = null;
    tuples.compactInto(getBuffers()[0]);

    while ((dm = getBuffers()[0].popAnyAsTMUsingTimeout()) != null) {

      /* broadcast message to multiple workers */
      for (int i = 0; i < numChannels(); i++) {
        try {
          writeMessage(i, dm);
        } catch (InterruptedException e) {
          throw new DbException(e);
        }
      }
    }
  }

  @Override
  protected final void childEOS() throws DbException {

    TransportMessage dm = null;
    int numChannels = super.numChannels();

    /* BroadcastProducer only uses getBuffers()[0] */
    while ((dm = getBuffers()[0].popAnyAsTM()) != null) {
      for (int i = 0; i < numChannels(); i++) {
        try {
          writeMessage(i, dm);
        } catch (InterruptedException e) {
          throw new DbException(e);
        }
      }
    }

    for (int i = 0; i < numChannels; i++) {
      super.channelEnds(i);
    }
  }

  @Override
  protected final void childEOI() throws DbException {

    TransportMessage dm = null;

    /* BroadcastProducer only uses getBuffers()[0] */
    while ((dm = getBuffers()[0].popAnyAsTM()) != null) {
      for (int i = 0; i < numChannels(); i++) {
        try {
          writeMessage(i, dm);
        } catch (InterruptedException e) {
          throw new DbException(e);
        }
      }
    }

    for (int i = 0; i < numChannels(); i++) {
      try {
        writeMessage(i, IPCUtils.EOI);
      } catch (InterruptedException e) {
        throw new DbException(e);
      }
    }

  }
}

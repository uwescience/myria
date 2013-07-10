package edu.washington.escience.myriad.parallel;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.operator.Operator;

/**
 * The producer part of the Collect Exchange operator.
 * 
 * The producer actively pushes the tuples generated by the child operator to the paired LocalMultiwayConsumer.
 * 
 */
public final class LocalMultiwayProducer extends Producer {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * @param child the child who provides data for this producer to distribute.
   * @param operatorIDs destination operators the data goes
   * */
  public LocalMultiwayProducer(final Operator child, final ExchangePairID[] operatorIDs) {
    super(child, operatorIDs);
  }

  @Override
  protected void consumeTuples(final TupleBatch tup) throws DbException {
    TupleBatchBuffer[] buffers = getBuffers();
    TupleBatch dm = null;
    tup.compactInto(buffers[0]);
    while ((dm = buffers[0].popAnyUsingTimeout()) != null) {
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
  protected void childEOS() throws DbException {
    TupleBatch dm = null;
    TupleBatchBuffer[] buffers = getBuffers();
    while ((dm = buffers[0].popAny()) != null) {
      for (int i = 0; i < numChannels(); i++) {
        try {
          writeMessage(i, dm);
        } catch (InterruptedException e) {
          throw new DbException(e);
        }
      }
    }

    for (int i = 0; i < numChannels(); i++) {
      super.channelEnds(i);
    }
  }

  @Override
  protected void childEOI() throws DbException {
    TupleBatch dm = null;
    TupleBatchBuffer[] buffers = getBuffers();
    while ((dm = buffers[0].popAny()) != null) {
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
        writeMessage(i, TupleBatch.eoiTupleBatch(getSchema()));
      } catch (InterruptedException e) {
        throw new DbException(e);
      }
    }
  }
}

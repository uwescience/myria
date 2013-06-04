package edu.washington.escience.myriad.parallel;

import org.jboss.netty.channel.Channel;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.util.IPCUtils;

/**
 * The producer part of the Shuffle Exchange operator.
 * 
 * ShuffleProducer distributes tuples to the workers according to some partition function (provided as a
 * PartitionFunction object during the ShuffleProducer's instantiation).
 * 
 */
public class LocalShuffleProducer extends Producer {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * the partition function.
   * */
  private final PartitionFunction<?, ?> partitionFunction;

  /**
   * @param child the child who provides data for this producer to distribute.
   * @param operatorIDs destination operators the data goes
   * @param pf the partition function
   * */
  public LocalShuffleProducer(final Operator child, final ExchangePairID[] operatorIDs, final PartitionFunction<?, ?> pf) {
    super(child, operatorIDs);
    partitionFunction = pf;
  }

  /**
   * @return the partition function I'm using.
   * */
  public final PartitionFunction<?, ?> getPartitionFunction() {
    return partitionFunction;
  }

  @Override
  protected final void consumeTuples(final TupleBatch tup) throws DbException {
    TupleBatchBuffer[] buffers = getBuffers();
    Channel[] ioChannels = getChannels();
    tup.partition(partitionFunction, buffers);
    TransportMessage dm = null;
    for (int p = 0; p < ioChannels.length; p++) {
      final TupleBatchBuffer etb = buffers[p];
      while ((dm = etb.popFilledAsTM()) != null) {
        try {
          writeMessage(ioChannels[p], dm);
        } catch (InterruptedException e) {
          throw new DbException(e);
        }
      }
    }
  }

  @Override
  protected final void childEOS() throws DbException {
    TransportMessage dm = null;
    TupleBatchBuffer[] buffers = getBuffers();
    Channel[] ioChannels = getChannels();
    for (int i = 0; i < ioChannels.length; i++) {
      while ((dm = buffers[i].popAnyAsTM()) != null) {
        try {
          writeMessage(ioChannels[i], dm);
        } catch (InterruptedException e) {
          throw new DbException(e);
        }
      }
    }
    for (Channel channel : ioChannels) {
      try {
        writeMessage(channel, IPCUtils.EOS);
      } catch (InterruptedException e) {
        throw new DbException(e);
      }
    }

  }

  @Override
  protected final void childEOI() throws DbException {
    TransportMessage dm = null;
    TupleBatchBuffer[] buffers = getBuffers();
    Channel[] ioChannels = getChannels();
    for (int i = 0; i < ioChannels.length; i++) {
      while ((dm = buffers[i].popAnyAsTM()) != null) {
        try {
          writeMessage(ioChannels[i], dm);
        } catch (InterruptedException e) {
          throw new DbException(e);
        }
      }
    }
    for (Channel channel : ioChannels) {
      try {
        writeMessage(channel, IPCUtils.EOI);
      } catch (InterruptedException e) {
        throw new DbException(e);
      }
    }
  }
}

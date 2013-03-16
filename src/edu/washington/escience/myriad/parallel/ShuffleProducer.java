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
public class ShuffleProducer extends Producer {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private final PartitionFunction<?, ?> partitionFunction;

  public ShuffleProducer(final Operator child, final ExchangePairID operatorID, final int[] workerIDs,
      final PartitionFunction<?, ?> pf) {
    super(child, operatorID, workerIDs);
    partitionFunction = pf;
  }

  public final PartitionFunction<?, ?> getPartitionFunction() {
    return partitionFunction;
  }

  @Override
  protected void consumeTuples(final TupleBatch tup) throws DbException {
    TupleBatchBuffer[] buffers = getBuffers();
    Channel[] ioChannels = getChannels();
    tup.partition(partitionFunction, buffers);
    TransportMessage dm = null;
    for (int p = 0; p < ioChannels.length; p++) {
      final TupleBatchBuffer etb = buffers[p];
      while ((dm = etb.popFilledAsTM(super.outputSeq[p])) != null) {
        super.outputSeq[p]++;
        ioChannels[p].write(dm);
      }
    }
  }

  @Override
  protected void childEOS() throws DbException {
    TransportMessage dm = null;
    TupleBatchBuffer[] buffers = getBuffers();
    Channel[] ioChannels = getChannels();
    for (int i = 0; i < ioChannels.length; i++) {
      while ((dm = buffers[i].popAnyAsTM(super.outputSeq[i])) != null) {
        super.outputSeq[i]++;
        ioChannels[i].write(dm);
      }
    }
    for (Channel channel : ioChannels) {
      channel.write(IPCUtils.EOS);
    }

  }

  @Override
  protected void childEOI() throws DbException {
    TransportMessage dm = null;
    TupleBatchBuffer[] buffers = getBuffers();
    Channel[] ioChannels = getChannels();
    for (int i = 0; i < ioChannels.length; i++) {
      while ((dm = buffers[i].popAnyAsTM(super.outputSeq[i])) != null) {
        super.outputSeq[i]++;
        ioChannels[i].write(dm);
      }
    }
    for (Channel channel : ioChannels) {
      channel.write(IPCUtils.EOI);
    }
  }
}

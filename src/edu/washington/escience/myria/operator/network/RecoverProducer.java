package edu.washington.escience.myria.operator.network;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.ipc.StreamOutputChannel;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * The producer part of the Collect Exchange operator.
 * 
 * The producer actively pushes the tuples generated by the child operator to the paired CollectConsumer.
 * 
 */
public final class RecoverProducer extends CollectProducer {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(RecoverProducer.class);

  /** the original producer. */
  private final Producer oriProducer;
  /** the channel index that this operator is recovering for. */
  private final int channelIndx;

  /**
   * @param child the child who provides data for this producer to distribute.
   * @param operatorID destination operator the data goes
   * @param collectConsumerWorkerID destination worker the data goes.
   * @param oriProducer the original producer.
   * @param channelIndx the channel index that this operator is recovering for. *
   * */
  public RecoverProducer(final Operator child, final ExchangePairID operatorID, final int collectConsumerWorkerID,
      final Producer oriProducer, final int channelIndx) {
    super(child, operatorID, collectConsumerWorkerID);
    this.oriProducer = oriProducer;
    this.channelIndx = channelIndx;
  }

  @Override
  protected void childEOS() throws DbException {
    writePartitionsIntoChannels(false, null);
    Preconditions.checkArgument(getChild() instanceof TupleSource);
    if (!oriProducer.eos()) {
      StreamOutputChannel<TupleBatch> tmp = getChannels()[0];
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("recovery task {} detach & attach channel {} old channel {} new channel {}", getOpName(), tmp
            .getID(), oriProducer.getChannels()[channelIndx], tmp);
      }
      oriProducer.getChannels()[channelIndx] = tmp;
      /* have to do this otherwise the channel will be released in resourceManager.cleanup() */
      getTaskResourceManager().removeOutputChannel(tmp);
      /* have to do this otherwise the channel will be released in Producer.cleanup() */
      getChannels()[0] = null;
      /* set the channel to be available again */
      oriProducer.getChannelsAvail()[channelIndx] = true;
      /* if the channel was disabled before crash, need to give the task a chance to enable it. */
      oriProducer.getTaskResourceManager().getFragment().notifyOutputEnabled(tmp.getID());
      /* if the task has no new input, but needs to produce potential EOSs & push TBs in its buffers out. */
      oriProducer.getTaskResourceManager().getFragment().notifyNewInput();
    } else {
      channelEnds(0);
    }
  }
}

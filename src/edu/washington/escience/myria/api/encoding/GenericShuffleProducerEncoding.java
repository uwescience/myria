package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Set;

import com.google.common.primitives.Ints;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.distribute.DistributeFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;

/** JSON wrapper for GenericShuffleProducer encoding. */
public class GenericShuffleProducerEncoding
    extends AbstractProducerEncoding<GenericShuffleProducer> {

  /** The distribute function. */
  @Required public DistributeFunction distributeFunction;

  /** Type of the buffer for recovery. */
  public StreamingStateEncoding<?> argBufferStateType;

  @Override
  public GenericShuffleProducer construct(final ConstructArgs args) {
    Set<Integer> workerIds = getRealWorkerIds();
    List<ExchangePairID> operatorIds = getRealOperatorIds();
    distributeFunction.setDestinations(workerIds.size(), operatorIds.size());
    GenericShuffleProducer producer =
        new GenericShuffleProducer(
            null,
            operatorIds.toArray(new ExchangePairID[] {}),
            Ints.toArray(workerIds),
            distributeFunction);
    if (argBufferStateType != null) {
      producer.setBackupBuffer(argBufferStateType);
    }
    return producer;
  }
}

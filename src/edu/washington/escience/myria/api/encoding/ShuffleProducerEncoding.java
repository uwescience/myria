package edu.washington.escience.myria.api.encoding;

import java.util.Set;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 *
 */
public class ShuffleProducerEncoding extends AbstractProducerEncoding<GenericShuffleProducer> {
  @Required public PartitionFunction argPf;
  public StreamingStateEncoding<?> argBufferStateType;

  @Override
  public GenericShuffleProducer construct(final ConstructArgs args) {
    Set<Integer> workerIds = getRealWorkerIds();
    argPf.setNumPartitions(workerIds.size());
    GenericShuffleProducer producer =
        new GenericShuffleProducer(
            null,
            MyriaUtils.getSingleElement(getRealOperatorIds()),
            MyriaUtils.integerSetToIntArray(workerIds),
            argPf);
    if (argBufferStateType != null) {
      if (argBufferStateType instanceof KeepMinValueStateEncoding) {
        producer.setBackupBufferAsMin(
            ((KeepMinValueStateEncoding) argBufferStateType).keyColIndices,
            ((KeepMinValueStateEncoding) argBufferStateType).valueColIndices);
      } else if (argBufferStateType instanceof KeepAndSortOnMinValueStateEncoding) {
        producer.setBackupBufferAsPrioritizedMin(
            ((KeepAndSortOnMinValueStateEncoding) argBufferStateType).keyColIndices,
            ((KeepAndSortOnMinValueStateEncoding) argBufferStateType).valueColIndices);
      } else if (argBufferStateType instanceof DupElimStateEncoding) {
        producer.setBackupBufferAsDupElim();
      } else if (argBufferStateType instanceof SimpleAppenderStateEncoding) {
        producer.setBackupBufferAsAppender();
      }
    }
    return producer;
  }
}

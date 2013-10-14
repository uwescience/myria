package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.GenericShuffleProducer;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 */
public class ShuffleProducerEncoding extends AbstractProducerEncoding<GenericShuffleProducer> {
  public String argChild;
  public String argOperatorId;
  public PartitionFunctionEncoding<?> argPf;
  public StreamingStateUpdaterEncoding<?> argBufferStateUpdater;
  private static final List<String> requiredArguments = ImmutableList.of("argChild", "argOperatorId", "argPf");

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public GenericShuffleProducer construct(Server server) {
    Set<Integer> workerIds = getRealWorkerIds();
    GenericShuffleProducer producer =
        new GenericShuffleProducer(null, MyriaUtils.getSingleElement(getRealOperatorIds()), MyriaUtils
            .integerCollectionToIntArray(workerIds), argPf.construct(workerIds.size()));
    if (argBufferStateUpdater != null) {
      if (argBufferStateUpdater instanceof KeepMinValueEncoding) {
        producer.setBackupBufferAsMin(((KeepMinValueEncoding) argBufferStateUpdater).keyColIndices,
            ((KeepMinValueEncoding) argBufferStateUpdater).valueColIndex);
      } else if (argBufferStateUpdater instanceof DupElimEncoding) {
        producer.setBackupBufferAsDupElim();
      }
    }
    return producer;
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }

  @Override
  protected void validateExtra() {
    argPf.validate();
  }

  @Override
  protected List<String> getOperatorIds() {
    return ImmutableList.of(argOperatorId);
  }
}
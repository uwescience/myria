package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.parallel.ShuffleProducer;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 */
public class ShuffleProducerEncoding extends OperatorEncoding<ShuffleProducer> {
  public String argChild;
  public Integer argOperatorId;
  public int[] argWorkerIds;
  public PartitionFunctionEncoding<?> argPf;
  private static final List<String> requiredArguments = ImmutableList.of("argChild", "argWorkerIds", "argOperatorId",
      "argPf");

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public ShuffleProducer construct() {
    return new ShuffleProducer(null, ExchangePairID.fromExisting(argOperatorId), argWorkerIds, argPf
        .construct(argWorkerIds.length));
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }

  @Override
  protected void validateExtra() {
    argPf.validate();
  }
}
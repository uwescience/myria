package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.parallel.ShuffleConsumer;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 */
public class ShuffleConsumerEncoding extends OperatorEncoding<ShuffleConsumer> {
  public Schema argSchema;
  public Integer argOperatorId;
  public int[] argWorkerIds;
  private static final List<String> requiredArguments = ImmutableList.of("argSchema", "argWorkerIds", "argOperatorId");

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    /* Do nothing; no children. */
  }

  @Override
  public ShuffleConsumer construct() {
    return new ShuffleConsumer(argSchema, ExchangePairID.fromExisting(argOperatorId), argWorkerIds);
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}
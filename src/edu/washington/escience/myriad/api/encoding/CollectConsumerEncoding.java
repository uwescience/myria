package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.ExchangePairID;

public class CollectConsumerEncoding extends OperatorEncoding<CollectConsumer> {
  public Schema argSchema;
  public int[] argWorkerIds;
  public Integer argOperatorId;
  private static final List<String> requiredArguments = ImmutableList.of("argSchema", "argWorkerIds", "argOperatorId");

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    /* Do nothing. */
  }

  @Override
  public CollectConsumer construct() {
    return new CollectConsumer(argSchema, ExchangePairID.fromExisting(argOperatorId), argWorkerIds);
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}
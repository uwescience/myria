package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.Consumer;
import edu.washington.escience.myriad.parallel.ExchangePairID;

public class ConsumerEncoding extends OperatorEncoding<Consumer> {
  public Schema argSchema;
  public int[] argWorkerIds;
  public Integer argOperatorId;
  private final static List<String> requiredArguments = ImmutableList.of("argSchema", "argWorkerIds", "argOperatorId");

  @Override
  public Consumer construct() {
    return new Consumer(argSchema, ExchangePairID.fromExisting(argOperatorId), argWorkerIds);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    /* Do nothing. */
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}
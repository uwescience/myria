package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.Consumer;
import edu.washington.escience.myriad.parallel.Server;
import edu.washington.escience.myriad.util.MyriaUtils;

public class ConsumerEncoding extends AbstractConsumerEncoding<Consumer> {
  public Schema argSchema;
  public int[] argWorkerIds;
  public String argOperatorId;
  private final static List<String> requiredArguments = ImmutableList.of("argSchema", "argOperatorId");

  @Override
  public Consumer construct(Server server) {
    return new Consumer(argSchema, MyriaUtils.getSingleElement(getRealOperatorIds()), MyriaUtils
        .integerCollectionToIntArray(getRealWorkerIds()));
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    /* Do nothing. */
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }

  @Override
  protected List<String> getOperatorIds() {
    return ImmutableList.of(argOperatorId);
  }
}
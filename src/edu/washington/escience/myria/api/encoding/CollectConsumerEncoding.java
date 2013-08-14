package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.CollectConsumer;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.util.MyriaUtils;

public class CollectConsumerEncoding extends AbstractConsumerEncoding<CollectConsumer> {
  public Schema argSchema;
  public String argOperatorId;
  private static final List<String> requiredArguments = ImmutableList.of("argSchema", "argOperatorId");

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    /* Do nothing. */
  }

  @Override
  public CollectConsumer construct(Server server) {
    return new CollectConsumer(argSchema, MyriaUtils.getSingleElement(getRealOperatorIds()), MyriaUtils
        .integerCollectionToIntArray(getRealWorkerIds()));
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
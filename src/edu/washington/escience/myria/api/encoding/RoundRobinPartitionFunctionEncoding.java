package edu.washington.escience.myria.api.encoding;

import java.util.List;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.parallel.RoundRobinPartitionFunction;

public class RoundRobinPartitionFunctionEncoding extends PartitionFunctionEncoding<RoundRobinPartitionFunction> {
  private static final List<String> requiredFields = ImmutableList.of();

  @Override
  public RoundRobinPartitionFunction construct(final int numPartitions) {
    return new RoundRobinPartitionFunction(numPartitions);
  }

  @Override
  protected List<String> getRequiredFields() {
    return requiredFields;
  }
}
package edu.washington.escience.myria.api.encoding;

import java.util.List;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.parallel.SingleFieldHashPartitionFunction;

public class SingleFieldPartitionFunctionEncoding extends PartitionFunctionEncoding<SingleFieldHashPartitionFunction> {
  public Integer index;
  private static final List<String> requiredFields = ImmutableList.of("index");

  @Override
  public SingleFieldHashPartitionFunction construct(final int numPartitions) {
    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(numPartitions, index);
    return pf;
  }

  @Override
  protected List<String> getRequiredFields() {
    return requiredFields;
  }
}
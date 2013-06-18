package edu.washington.escience.myriad.api.encoding;

import java.util.List;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.parallel.SingleFieldHashPartitionFunction;

public class SingleFieldPartitionFunctionEncoding extends PartitionFunctionEncoding<SingleFieldHashPartitionFunction> {
  public Integer index;
  private static final List<String> requiredArguments = ImmutableList.of("index");

  @Override
  public SingleFieldHashPartitionFunction construct(final int numPartitions) {
    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(numPartitions);
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, index);
    return pf;
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}
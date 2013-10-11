package edu.washington.escience.myria.api.encoding;

import java.util.List;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.parallel.MultiFieldHashPartitionFunction;

public class MultiFieldPartitionFunctionEncoding extends PartitionFunctionEncoding<MultiFieldHashPartitionFunction> {

  public int[] index;
  private static final List<String> requiredFields = ImmutableList.of("index");

  @Override
  public MultiFieldHashPartitionFunction construct(int numPartitions) {
    MultiFieldHashPartitionFunction pf = new MultiFieldHashPartitionFunction(numPartitions, index);
    return pf;
  }

  @Override
  protected List<String> getRequiredFields() {
    return requiredFields;
  }
}
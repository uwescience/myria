package edu.washington.escience.myria.api.encoding;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import edu.washington.escience.myria.parallel.PartitionFunction;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @Type(value = RoundRobinPartitionFunctionEncoding.class, name = "RoundRobin"),
    @Type(value = MultiFieldPartitionFunctionEncoding.class, name = "MultiFieldHash"),
    @Type(value = SingleFieldPartitionFunctionEncoding.class, name = "SingleFieldHash") })
public abstract class PartitionFunctionEncoding<T extends PartitionFunction<?, ?>> extends MyriaApiEncoding {
  /**
   * @param numPartitions the number of ways to partition the data.
   * @return the instantiated partition function.
   */
  public abstract T construct(int numPartitions);
}
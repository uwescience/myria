package edu.washington.escience.myriad.api.encoding;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonSubTypes.Type;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import edu.washington.escience.myriad.parallel.PartitionFunction;

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
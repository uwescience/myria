package edu.washington.escience.myriad.api.encoding;

import java.util.List;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonSubTypes.Type;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.parallel.PartitionFunction;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @Type(value = MultiFieldPartitionFunctionEncoding.class, name = "MultiFieldHash"),
    @Type(value = SingleFieldPartitionFunctionEncoding.class, name = "SingleFieldHash") })
public abstract class PartitionFunctionEncoding<T extends PartitionFunction<?, ?>> extends MyriaApiEncoding {
  public String type;

  /**
   * @param numPartitions the number of ways to partition the data.
   * @return the instantiated partition function.
   */
  public abstract T construct(int numPartitions);

  /**
   * @return the list of required arguments for this PartitionFunctionEncoding.
   */
  protected abstract List<String> getRequiredArguments();

  @Override
  protected List<String> getRequiredFields() {
    return new ImmutableList.Builder<String>().addAll(getRequiredArguments()).build();
  }
}
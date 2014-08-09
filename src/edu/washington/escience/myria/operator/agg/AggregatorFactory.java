package edu.washington.escience.myria.operator.agg;

import java.io.Serializable;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import edu.washington.escience.myria.Schema;

/**
 * Creates instances of the {@link Aggregator} class.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @Type(value = SingleColumnAggregatorFactory.class, name = "SingleColumn") })
public interface AggregatorFactory extends Serializable {
  /**
   * Create a new aggregator for tuples of the specified schema.
   * 
   * @param inputSchema the schema that incoming tuples will take.
   * @return a new aggregator for tuples of the specified schema.
   */
  Aggregator get(@Nonnull Schema inputSchema);
}

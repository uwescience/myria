package edu.washington.escience.myria.operator.agg;

import java.io.Serializable;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;

/**
 * Creates instances of the {@link Aggregator} class.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @Type(value = CountAllAggregatorFactory.class, name = "CountAll"),
    @Type(value = SingleColumnAggregatorFactory.class, name = "SingleColumn"),
    @Type(value = UserDefinedAggregatorFactory.class, name = "UserDefined") })
public interface AggregatorFactory extends Serializable {
  /**
   * Create a new aggregator for tuples of the specified schema.
   * 
   * @param inputSchema the schema that incoming tuples will take.
   * @return a new aggregator for tuples of the specified schema.
   * @throws DbException if there is an error creating the aggregator.
   */
  @Nonnull
  Aggregator get(Schema inputSchema) throws DbException;

  /**
   * Returns the schema of the aggregates over the specified input tuples.
   * 
   * @param inputSchema the schema of the input tuples.
   * @return the schema of the aggregates over the specified input tuples.
   */
  @Nonnull
  Schema getResultSchema(Schema inputSchema);
}

package edu.washington.escience.myria.api.encoding;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.agg.AggregatorFactory;
import edu.washington.escience.myria.operator.agg.MultiGroupByAggregate;

public class MultiGroupByAggregateEncoding extends UnaryOperatorEncoding<MultiGroupByAggregate> {

  @Required
  public int[] argGroupFields;
  @Required
  public AggregatorFactory[] aggregators;

  @Override
  public MultiGroupByAggregate construct(@Nonnull ConstructArgs args) {
    return new MultiGroupByAggregate(null, argGroupFields, aggregators);
  }
}

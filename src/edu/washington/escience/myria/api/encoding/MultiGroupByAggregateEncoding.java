package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.agg.AggregatorFactory;
import edu.washington.escience.myria.operator.agg.MultiGroupByAggregate;

public class MultiGroupByAggregateEncoding extends UnaryOperatorEncoding<MultiGroupByAggregate> {

  @Required public int[] argGroupFields;
  @Required public AggregatorFactory[] aggregators;

  @Override
  public MultiGroupByAggregate construct(ConstructArgs args) {
    return new MultiGroupByAggregate(null, argGroupFields, aggregators);
  }
}

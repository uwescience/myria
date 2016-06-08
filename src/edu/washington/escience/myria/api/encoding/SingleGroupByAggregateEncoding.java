package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.agg.AggregatorFactory;
import edu.washington.escience.myria.operator.agg.SingleGroupByAggregate;

public class SingleGroupByAggregateEncoding extends UnaryOperatorEncoding<SingleGroupByAggregate> {

  @Required public AggregatorFactory[] aggregators;
  @Required public int argGroupField;

  @Override
  public SingleGroupByAggregate construct(ConstructArgs args) {
    return new SingleGroupByAggregate(null, argGroupField, aggregators);
  }
}

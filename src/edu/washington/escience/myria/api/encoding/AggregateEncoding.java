package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.agg.Aggregate;
import edu.washington.escience.myria.operator.agg.AggregatorFactory;

public class AggregateEncoding extends UnaryOperatorEncoding<Aggregate> {
  @Required public AggregatorFactory[] aggregators;

  @Override
  public Aggregate construct(ConstructArgs args) {
    return new Aggregate(null, aggregators);
  }
}

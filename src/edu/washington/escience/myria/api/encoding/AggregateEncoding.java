package edu.washington.escience.myria.api.encoding;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.agg.Aggregate;
import edu.washington.escience.myria.operator.agg.AggregatorFactory;

public class AggregateEncoding extends UnaryOperatorEncoding<Aggregate> {
  @Required
  public AggregatorFactory[] aggregators;

  @Override
  public Aggregate construct(@Nonnull final ConstructArgs args) {
    return new Aggregate(null, aggregators);
  }
}
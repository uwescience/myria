package edu.washington.escience.myria.api.encoding;

import com.google.common.base.MoreObjects;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.agg.AggregatorFactory;
import edu.washington.escience.myria.operator.agg.SingleGroupByAggregate;

public class SingleGroupByAggregateEncoding extends UnaryOperatorEncoding<SingleGroupByAggregate> {

  @Required
  public AggregatorFactory[] aggregators;
  @Required
  public int argGroupField;
  public Boolean isCombiner;

  @Override
  public SingleGroupByAggregate construct(final ConstructArgs args) {
    isCombiner = MoreObjects.firstNonNull(isCombiner, Boolean.FALSE);
    return new SingleGroupByAggregate(null, argGroupField, isCombiner, aggregators);
  }
}

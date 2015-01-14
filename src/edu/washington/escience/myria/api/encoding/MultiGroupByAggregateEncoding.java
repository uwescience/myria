package edu.washington.escience.myria.api.encoding;

import com.google.common.base.MoreObjects;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.agg.AggregatorFactory;
import edu.washington.escience.myria.operator.agg.MultiGroupByAggregate;

public class MultiGroupByAggregateEncoding extends UnaryOperatorEncoding<MultiGroupByAggregate> {

  @Required
  public int[] argGroupFields;
  @Required
  public AggregatorFactory[] aggregators;
  public Boolean isCombiner;

  @Override
  public MultiGroupByAggregate construct(final ConstructArgs args) {
    isCombiner = MoreObjects.firstNonNull(isCombiner, Boolean.FALSE);
    return new MultiGroupByAggregate(null, argGroupFields, isCombiner, aggregators);
  }
}

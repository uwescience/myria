package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.agg.AggregatorFactory;
import edu.washington.escience.myria.operator.agg.EStepAggregate;

public class EStepAggregateEncoding extends
		UnaryOperatorEncoding<EStepAggregate> {

	@Required
	public AggregatorFactory[] aggregators;
	@Required
	public int argGroupField;

	@Required
	public int argNumDimensions;

	@Required
	public int argNumComponents;

	@Override
	public EStepAggregate construct(ConstructArgs args) {
		return new EStepAggregate(null, argGroupField, aggregators,
				argNumDimensions, argNumComponents);
	}
}

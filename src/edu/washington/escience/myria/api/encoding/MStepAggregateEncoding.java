package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.agg.AggregatorFactory;
import edu.washington.escience.myria.operator.agg.MStepAggregate;

public class MStepAggregateEncoding extends
		UnaryOperatorEncoding<MStepAggregate> {

	@Required
	public AggregatorFactory[] aggregators;
	@Required
	public int argGroupField;

	@Override
	public MStepAggregate construct(ConstructArgs args) {
		return new MStepAggregate(null, argGroupField, aggregators);
	}
}

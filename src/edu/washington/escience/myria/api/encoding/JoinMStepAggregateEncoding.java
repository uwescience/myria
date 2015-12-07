package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.agg.AggregatorFactory;
import edu.washington.escience.myria.operator.agg.JoinMStepAggregate;

public class JoinMStepAggregateEncoding extends
		UnaryOperatorEncoding<JoinMStepAggregate> {

	@Required
	public AggregatorFactory[] aggregators;
	@Required
	public int argGroupField;

	@Required
	public int argNumDimensions;

	@Required
	public int argNumComponents;

	@Override
	public JoinMStepAggregate construct(ConstructArgs args) {
		return new JoinMStepAggregate(null, argGroupField, aggregators,
				argNumDimensions, argNumComponents);
	}
}

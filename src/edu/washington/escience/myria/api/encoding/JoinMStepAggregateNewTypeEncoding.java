package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.agg.AggregatorFactory;
import edu.washington.escience.myria.operator.agg.JoinMStepAggregateNewType;

public class JoinMStepAggregateNewTypeEncoding extends
		UnaryOperatorEncoding<JoinMStepAggregateNewType> {

	@Required
	public AggregatorFactory[] aggregators;
	@Required
	public int argGroupField;

	@Required
	public int argNumDimensions;

	@Required
	public int argNumComponents;

	@Override
	public JoinMStepAggregateNewType construct(ConstructArgs args) {
		return new JoinMStepAggregateNewType(null, argGroupField, aggregators,
				argNumDimensions, argNumComponents);
	}
}

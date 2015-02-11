package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.operator.ApplyEStep;

public class ApplyEStepEncoding extends UnaryOperatorEncoding<ApplyEStep> {

	@Required
	public List<Expression> emitExpressions;

	@Override
	public ApplyEStep construct(ConstructArgs args) {
		return new ApplyEStep(null, emitExpressions);
	}
}

package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.operator.ApplyOnes;

public class ApplyOnesEncoding extends UnaryOperatorEncoding<ApplyOnes> {

	@Required
	public List<Expression> emitExpressions;

	@Override
	public ApplyOnes construct(ConstructArgs args) {
		return new ApplyOnes(null, emitExpressions);
	}
}

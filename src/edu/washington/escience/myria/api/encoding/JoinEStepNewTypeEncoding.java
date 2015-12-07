package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.JoinEStepNewType;
import edu.washington.escience.myria.operator.RightHashJoin;

/**
 * 
 * Encoding for {@link RightHashJoin}.
 * 
 */
public class JoinEStepNewTypeEncoding extends
		BinaryOperatorEncoding<JoinEStepNewType> {
	public List<String> argColumnNames;
	@Required
	public int[] argColumns1;
	@Required
	public int[] argColumns2;
	@Required
	public int[] argSelect1;
	@Required
	public int[] argSelect2;

	@Override
	public JoinEStepNewType construct(ConstructArgs args) {
		return new JoinEStepNewType(argColumnNames, null, null, argColumns1,
				argColumns2, argSelect1, argSelect2);
	}
}

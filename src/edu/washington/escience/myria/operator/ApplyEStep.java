package edu.washington.escience.myria.operator;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.jblas.Decompose;
import org.jblas.DoubleMatrix;
import org.jblas.Solve;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.evaluate.ConstantEvaluator;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;
import edu.washington.escience.myria.expression.evaluate.GenericEvaluator;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Generic apply operator.
 */
public class ApplyEStep extends UnaryOperator {
	/***/
	private static final long serialVersionUID = 1L;

	/**
	 * Create logger for info logging below.
	 */
	private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory
			.getLogger(ApplyEStep.class);

	/**
	 * List of expressions that will be used to create the output.
	 */
	private ImmutableList<Expression> emitExpressions;

	/**
	 * One evaluator for each expression in {@link #emitExpressions}.
	 */
	private ArrayList<GenericEvaluator> emitEvaluators;

	/**
	 * @return the {@link #emitExpressions}
	 */
	protected ImmutableList<Expression> getEmitExpressions() {
		return emitExpressions;
	}

	/**
	 * @return the {@link #emitEvaluators}
	 */
	public ArrayList<GenericEvaluator> getEmitEvaluators() {
		return emitEvaluators;
	}

	/**
	 * 
	 * @param child
	 *            child operator that data is fetched from
	 * @param emitExpressions
	 *            expression that created the output
	 */
	public ApplyEStep(final Operator child,
			final List<Expression> emitExpressions) {
		super(child);
		if (emitExpressions != null) {
			setEmitExpressions(emitExpressions);
		}

		LOGGER.info("From ApplyEStep - Expectations step for EM.");
		DoubleMatrix sigma = new DoubleMatrix(new double[][] { { 2.0, 1.0 },
				{ 1.0, 2.0 } });
		DoubleMatrix identitiyMatrix = new DoubleMatrix(new double[][] {
				{ 1.0, 0.0 }, { 0.0, 1.0 } });

		DoubleMatrix x = new DoubleMatrix(new double[][] { { 0. }, { 0. } });
		DoubleMatrix mu = new DoubleMatrix(new double[][] { { 1. }, { 2. } });
		// The amplitude of the k'th Gaussian, pi_k
		double amp = 1;

		// Input points x, Gaussian means mu, Gaussian covariance V
		DoubleMatrix sigmaInv = Solve.solveSymmetric(sigma, identitiyMatrix);
		DoubleMatrix xSubMu = x.sub(mu);

		// Compute Log of Gaussian kernal, -1/2 * (x - mu).T * V * (x - mu)
		double kernal = xSubMu.transpose().mmul(sigmaInv.mmul(xSubMu))
				.mmul(-.5).get(0);

		// Compute Gaussian determinant using LU decomposition:
		Decompose.LUDecomposition<DoubleMatrix> lu = Decompose.lu(sigma);
		// The determinant is the product of rows of U in the LU decomposition
		double det = 1;
		for (int i = 0; i < lu.u.rows; i++) {
			det *= lu.u.get(i, i);
		}

		// Compute the log of the Gaussian constant: ln[ 1 / sqrt( (2*PI)^d *
		// det_Sigma ) ]
		double logConst = Math.log((1. / Math.sqrt(Math.pow(2 * Math.PI, 2)
				* det)));

		// Now we have the components of the log(p(x | theta))
		double logP = logConst + kernal;

		// Final output: ln pi_k + ln p(x_i | theta_k(t-1))
		double output = Math.log(amp) + logP;
		LOGGER.info("EM output = " + output);

		// System.out.println("Hello world, this is Ryan.");
		// throw new RuntimeException("HELLO");
	}

	/**
	 * @param emitExpressions
	 *            the emit expressions for each column
	 */
	private void setEmitExpressions(final List<Expression> emitExpressions) {
		this.emitExpressions = ImmutableList.copyOf(emitExpressions);
	}

	@Override
	protected TupleBatch fetchNextReady() throws DbException,
			InvocationTargetException {
		Operator child = getChild();
		LOGGER.info("FNR called once");

		if (child.eoi() || child.eos()) {
			return null;
		}

		TupleBatch tb = child.nextReady();
		if (tb == null) {
			return null;
		}

		// // Check to see if we've read input
		// if (tb != null) {
		// for (int row = 0; row < tb.numTuples(); ++row) {
		// List<? extends Column<?>> inputColumns = tb.getDataColumns();
		// for (int column = 0; column < tb.numColumns(); ++column) {
		// LOGGER.info("Type of column is: "
		// + inputColumns.get(column).getType());
		// }
		// }
		// }
		// // The rest does the normal apply step

		List<Column<?>> output = Lists.newLinkedList();
		for (GenericEvaluator evaluator : emitEvaluators) {
			output.add(evaluator.evaluateColumn(tb));
		}
		return new TupleBatch(getSchema(), output);
	}

	@Override
	protected void init(final ImmutableMap<String, Object> execEnvVars)
			throws DbException {
		Preconditions.checkNotNull(emitExpressions);

		Schema inputSchema = Objects.requireNonNull(getChild().getSchema());

		emitEvaluators = new ArrayList<>(emitExpressions.size());
		final ExpressionOperatorParameter parameters = new ExpressionOperatorParameter(
				inputSchema, getNodeID());
		for (Expression expr : emitExpressions) {
			GenericEvaluator evaluator;
			if (expr.isConstant()) {
				evaluator = new ConstantEvaluator(expr, parameters);
			} else {
				evaluator = new GenericEvaluator(expr, parameters);
			}
			if (evaluator.needsCompiling()) {
				evaluator.compile();
			}
			Preconditions.checkArgument(!evaluator.needsState());
			emitEvaluators.add(evaluator);
		}
	}

	/**
	 * @param evaluators
	 *            the evaluators to set
	 */
	public void setEvaluators(final ArrayList<GenericEvaluator> evaluators) {
		emitEvaluators = evaluators;
	}

	@Override
	public Schema generateSchema() {
		if (emitExpressions == null) {
			return null;
		}
		Operator child = getChild();
		if (child == null) {
			return null;
		}
		Schema inputSchema = child.getSchema();
		if (inputSchema == null) {
			return null;
		}

		ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
		ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();

		for (Expression expr : emitExpressions) {
			typesBuilder
					.add(expr.getOutputType(new ExpressionOperatorParameter(
							inputSchema)));
			namesBuilder.add(expr.getOutputName());
		}
		return new Schema(typesBuilder.build(), namesBuilder.build());
		// Manually return a new schema
		// List<Type> types = new ArrayList<Type>();
		// types.add(Type.LONG_TYPE);
		// List<String> names = new ArrayList<String>();
		// names.add("ones");
		// return new Schema(typesBuilder.build(), namesBuilder.build());
	}
}

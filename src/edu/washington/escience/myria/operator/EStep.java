package edu.washington.escience.myria.operator;

import java.util.List;

import org.jblas.Decompose;
import org.jblas.DoubleMatrix;
import org.jblas.Solve;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.DoubleColumnBuilder;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * EStep is an operator that takes input tuples representing a point and
 * Gaussian, evaluates the Gaussian at that point, and appends that value as a
 * new column.
 */
public final class EStep extends UnaryOperator {

	/** Required for Java serialization. */
	private static final long serialVersionUID = 1L;

	/**
	 * Create logger for info logging below.
	 */
	private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory
			.getLogger(ApplyEStep.class);

	/**
	 * Which columns to sort the tuples by.
	 */
	private final String columnName = "EStepVal";

	/**
	 * Constructor accepts a predicate to apply and a child operator to read
	 * tuples to filter from.
	 * 
	 * @param child
	 *            The child operator
	 */
	public EStep(final Operator child) {
		super(child);
	}

	@Override
	protected TupleBatch fetchNextReady() throws DbException {
		Operator child = getChild();

		TupleBatch tb = child.nextReady();
		if (tb == null) {
			return null;
		}

		// Create new column builder to which to append output
		DoubleColumnBuilder builder = new DoubleColumnBuilder();

		for (int row = 0; row < tb.numTuples(); ++row) {
			List<? extends Column<?>> inputColumns = tb.getDataColumns();
			// for (int column = 0; column < tb.numColumns(); ++column) {
			// LOGGER.info("Column value is: "
			// + inputColumns.get(column).getLong(row));
			// }
			long pid = inputColumns.get(0).getLong(row);
			double x11 = inputColumns.get(1).getDouble(row);
			double x21 = inputColumns.get(2).getDouble(row);
			double r1 = inputColumns.get(3).getDouble(row);
			double r2 = inputColumns.get(4).getDouble(row);
			long gid = inputColumns.get(5).getLong(row);
			double pi = inputColumns.get(6).getDouble(row);
			double mu11 = inputColumns.get(7).getDouble(row);
			double mu21 = inputColumns.get(8).getDouble(row);
			double cov11 = inputColumns.get(9).getDouble(row);
			double cov12 = inputColumns.get(10).getDouble(row);
			double cov21 = inputColumns.get(11).getDouble(row);
			double cov22 = inputColumns.get(12).getDouble(row);
			double output = getPartialResponsibility(pid, x11, x21, r1, r2,
					gid, pi, mu11, mu21, cov11, cov12, cov21, cov22);
			LOGGER.info("Column value is: " + output);
			builder.appendDouble(output);
		}

		for (int i = 0; i < tb.numTuples(); ++i) {
		}
		return tb.appendColumn(columnName, builder.build());
	}

	@Override
	protected void init(final ImmutableMap<String, Object> execEnvVars)
			throws DbException {
		LOGGER.info("From FilterEStep - Reached init.");
	}

	@Override
	public Schema generateSchema() {
		final Operator child = getChild();
		if (child == null) {
			return null;
		}
		final Schema childSchema = child.getSchema();
		if (childSchema == null) {
			return null;
		}
		return Schema.appendColumn(childSchema, Type.DOUBLE_TYPE, columnName);

	}

	private double getPartialResponsibility(long pid, double x11, double x21,
			double r1, double r2, long gid, double pi, double mu11,
			double mu21, double cov11, double cov12, double cov21, double cov22) {
		// Roll up the input: a 2D point and its pid,
		// a 2D Gaussian mean and cov. matrix, gid.
		// A copy of the computation in SimpleGMMEStepWithRollup.java

		// INPUT: pid, x, gid, pi, mu, cov, dimension

		// Eventually we'll need the dimension parameter to somehow auto-roll
		// The matrices
		// int dimension = 2;

		// OUTPUT to be applied:
		double rik_loglhs;

		// The amplitude of the k'th Gaussian, pi_k
		double amp = pi;

		// Roll up data into array of doubles
		double[][] sigmaArray = new double[][] { { cov11, cov12 },
				{ cov21, cov22 } };
		double[][] xArray = new double[][] { { x11 }, { x21 } };
		double[][] muArray = new double[][] { { mu11 }, { mu21 } };

		// Compute Log of Gaussian kernal, -1/2 * (x - mu).T * V * (x - mu)
		double kernal = getKernalJblas(xArray, muArray, sigmaArray);

		DoubleMatrix Sigma = new DoubleMatrix(sigmaArray);
		// Compute Gaussian determinant using LU decomposition:
		Decompose.LUDecomposition<DoubleMatrix> LU = Decompose.lu(Sigma);
		// The determinant is the product of rows of U in the LU decomposition
		double det = 1;
		for (int i = 0; i < LU.u.rows; i++) {
			det *= LU.u.get(i, i);
		}
		// System.out.println("Determinant = " + det);
		// TODO, sometimes det is the negative of what it should be. This is
		// likely
		// due to our factorization.
		det = Math.abs(det);

		// Compute the log of the Gaussian constant: ln[ 1 / sqrt( (2*PI)^d *
		// det_Sigma ) ]
		double logConst = Math.log((1. / Math.sqrt(Math.pow(2 * Math.PI, 2)
				* det)));

		// Now we have the components of the log(p(x | theta))
		double log_p = logConst + kernal;

		// Final output: ln pi_k + ln p(x_i | theta_k(t-1))
		rik_loglhs = Math.log(amp) + log_p;

		// OUTPUT: pid, x, gid, pi, mu, cov, dimension, rik_rhs
		return rik_loglhs;
	}

	// Computes Log of Gaussian kernal, -1/2 * (x - mu).T * V * (x - mu)
	private double getKernalJblas(double[][] xArray, double[][] muArray,
			double[][] sigmaArray) {
		// Input points x, Gaussian means mu, Gaussian covariance V
		DoubleMatrix x = new DoubleMatrix(xArray);
		DoubleMatrix mu = new DoubleMatrix(muArray);

		// Get the inverse of the covariance matrix
		DoubleMatrix Sigma = new DoubleMatrix(sigmaArray);
		DoubleMatrix I = DoubleMatrix.eye(Sigma.columns);
		// DoubleMatrix I = new DoubleMatrix(new double[][] { { 1.0, 0.0 },
		// { 0.0, 1.0 } });
		DoubleMatrix SigmaInv = Solve.solveSymmetric(Sigma, I);

		// Get the difference between x and mu
		DoubleMatrix x_mu = x.sub(mu);

		// Compute Log of Gaussian kernal, -1/2 * (x - mu).T * V * (x - mu)
		return x_mu.transpose().mmul(SigmaInv.mmul(x_mu)).mmul(-.5).get(0);
	}
}

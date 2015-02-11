package edu.washington.escience.myria.operator.agg;

import java.util.ArrayList;
import java.util.Objects;

import org.jblas.DoubleMatrix;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator.AggregationOp;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * An aggregator for a column of primitive type.
 */
public class MStepAggregator implements Aggregator {

	/** Required for Java serialization. */
	private static final long serialVersionUID = 1L;

	/**
	 * Which column of the input to aggregate over. This is the partial
	 * responsibility column.
	 */
	private final int column;

	/** The actual aggregator doing the work. */
	// private final PrimitiveAggregator agg;

	/** A list of values to combine. **/
	private final ArrayList<Double> vals;

	private double[] x;

	private final double[] partialResponsibilities;
	// private int valIndex;

	private final int numComponents = 2;

	private final int numDimensions = 2;

	/**
	 * Variable number of points determined by points in each group.
	 */
	private int nPoints;

	/** The column index of the gid (Gaussian component id) in the input table **/
	private final int gidColumn = 5;

	private double r_k_partial;

	private DoubleMatrix mu_k_partial;

	private DoubleMatrix sigma_k_partial;

	/**
	 * A wrapper for the {@link PrimitiveAggregator} implementations like
	 * {@link IntegerAggregator}.
	 * 
	 * @param inputSchema
	 *            the schema of the input tuples.
	 * @param column
	 *            which column of the input to aggregate over.
	 * @param aggOps
	 *            which aggregate operations are requested. See
	 *            {@link PrimitiveAggregator}.
	 */
	public MStepAggregator(final Schema inputSchema, final int column,
			final AggregationOp[] aggOps) {
		Objects.requireNonNull(inputSchema, "inputSchema");
		this.column = column;
		// Objects.requireNonNull(aggOps, "aggOps");
		// agg = AggUtils.allocate(inputSchema.getColumnType(column),
		// inputSchema.getColumnName(column), aggOps);
		vals = new ArrayList<Double>();
		partialResponsibilities = new double[numComponents];
		x = null;
		// valIndex = 0;

		nPoints = 0;

		r_k_partial = 0.0;

		mu_k_partial = DoubleMatrix.zeros(numDimensions, 1);

		sigma_k_partial = DoubleMatrix.zeros(numDimensions, numDimensions);

	}

	@Override
	public void add(final ReadableTable from) {
		// Preconditions.checkState(agg != null,
		// "agg should not be null. Did you call getResultSchema yet?");
		// agg.add(from, column);
		// vals[valIndex] = from.getDouble(column, row)
	}

	@Override
	public void addRow(final ReadableTable from, final int row) {
		// Preconditions.checkState(agg != null,
		// "agg should not be null. Did you call getResultSchema yet?");
		// agg.add(from, column, row);

		// The gaussian id also serves as the index to the responsibility array
		long gid = from.getLong(gidColumn, row);
		partialResponsibilities[(int) gid] = from.getDouble(column, row);
		if (x == null) {
			x = new double[numDimensions];
			for (int i = 0; i < numDimensions; i++) {
				x[i] = from.getDouble(1 + i, row);
			}
		}

		vals.add(from.getDouble(column, row) * from.getDouble(column - 1, row)); // should
																					// be

		nPoints += 1;

		double r_ik = from.getDouble((int) gid + 1 + numComponents, row);

		double x11 = from.getDouble(1, row);
		double x21 = from.getDouble(2, row);
		//
		DoubleMatrix x = new DoubleMatrix(new double[][] { { x11 }, { x21 } });
		//
		// // Do updates
		r_k_partial += r_ik;
		mu_k_partial = mu_k_partial.add(x.mul(r_ik));
		//
		sigma_k_partial = sigma_k_partial.add(x.mmul(x.transpose()).mul(r_ik));

	}

	@Override
	public void getResult(final AppendableTable dest, final int destColumn) {
		// Preconditions.checkState(agg != null,
		// "agg should not be null. Did you call getResultSchema yet?");
		// agg.getResult(dest, destColumn);
		double sum = 0;
		for (double d : vals) {
			sum += d;
		}

		double logNormalization = logsumexp(partialResponsibilities);

		// // Loop through and output the x vector
		// for (int i = 0; i < x.length; i++) {
		// dest.putDouble(destColumn + i, x[i]);
		// }
		//
		// // Loop through the responsiblities and output them
		// for (int i = 0; i < partialResponsibilities.length; i++) {
		// dest.putDouble(destColumn + i + numDimensions,
		// Math.exp(partialResponsibilities[i] - logNormalization));
		// }

		// dest.putDouble(destColumn, sum * 10);
		// dest.putDouble(destColumn + 1, sum * 10);

		// r_k_partial is now r_k
		double r_k = r_k_partial;

		double pi = r_k / nPoints;

		DoubleMatrix mu = mu_k_partial.dup();
		mu = mu.div(r_k);

		DoubleMatrix sigma = sigma_k_partial.dup();
		sigma = sigma.div(r_k);
		sigma = sigma.sub(mu.mmul(mu.transpose()));

		// double pi = inputColumns.get(6).getDouble(row);
		double mu11 = mu.get(0, 0);
		double mu21 = mu.get(1, 0);
		double cov11 = sigma.get(0, 0);
		double cov12 = sigma.get(0, 1);
		double cov21 = sigma.get(1, 0);
		double cov22 = sigma.get(1, 1);

		dest.putDouble(destColumn + 0, pi);
		dest.putDouble(destColumn + 1, mu11);
		dest.putDouble(destColumn + 2, mu21);
		dest.putDouble(destColumn + 3, cov11);
		dest.putDouble(destColumn + 4, cov12);
		dest.putDouble(destColumn + 5, cov21);
		dest.putDouble(destColumn + 6, cov22);

	}

	@Override
	public Schema getResultSchema() {
		// For whatever reason, this is inconsistent with returnschema in
		// EStepAggregate, which
		// takes precedence
		// return agg.getResultSchema();
		final ImmutableList.Builder<Type> types = ImmutableList.builder();
		final ImmutableList.Builder<String> names = ImmutableList.builder();
		types.add(Type.DOUBLE_TYPE);
		names.add("irrelevant");
		types.add(Type.DOUBLE_TYPE);
		names.add("irrelevant2");
		types.add(Type.DOUBLE_TYPE);
		names.add("irrelevant3");
		types.add(Type.DOUBLE_TYPE);
		names.add("irrelevant4");
		types.add(Type.DOUBLE_TYPE);
		names.add("irrelevant5");
		types.add(Type.DOUBLE_TYPE);
		names.add("irrelevant6");
		types.add(Type.DOUBLE_TYPE);
		names.add("irrelevant7");

		return new Schema(types, names);
	}

	/** Calculates the log-sum-exp of the double matrix of elements. **/
	private static double logsumexp(double[] elements) {
		// the max term
		double maxElement = Double.NEGATIVE_INFINITY;

		// Find the max among elements
		for (double e : elements) {
			if (e > maxElement) {
				maxElement = e;
			}
		}

		// Get the adjusted sum
		double sumTerms = 0;
		for (double e : elements) {
			sumTerms += Math.exp(e - maxElement);
		}

		return maxElement + Math.log(sumTerms);

	}
}

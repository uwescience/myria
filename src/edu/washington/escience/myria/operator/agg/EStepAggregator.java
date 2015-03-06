package edu.washington.escience.myria.operator.agg;

import java.util.Objects;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator.AggregationOp;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * An aggregator for a column of primitive type.
 */
public class EStepAggregator implements Aggregator {

	/** Required for Java serialization. */
	private static final long serialVersionUID = 1L;

	/**
	 * Which column of the input to aggregate over. This is the partial
	 * responsibility column.
	 */
	private final int respColumn;

	/** The actual aggregator doing the work. */
	// private final PrimitiveAggregator agg;

	private double[] x;

	private final double[] partialResponsibilities;
	// private int valIndex;

	private final int numComponents;

	private final int numDimensions;

	/** The column index of the gid (Gaussian component id) in the input table **/
	private final int gidColumn;

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
	public EStepAggregator(final Schema inputSchema, final int column,
			final AggregationOp[] aggOps, int numDimensions, int numComponents) {
		Objects.requireNonNull(inputSchema, "inputSchema");
		respColumn = column;
		// Objects.requireNonNull(aggOps, "aggOps");
		// agg = AggUtils.allocate(inputSchema.getColumnType(column),
		// inputSchema.getColumnName(column), aggOps);
		partialResponsibilities = new double[numComponents];
		x = null;

		this.numDimensions = numDimensions;
		this.numComponents = numComponents;
		gidColumn = 1 + numDimensions + numComponents;
		// valIndex = 0;
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
		partialResponsibilities[(int) gid] = from.getDouble(respColumn, row);
		if (x == null) {
			x = new double[numDimensions];
			for (int i = 0; i < numDimensions; i++) {
				x[i] = from.getDouble(1 + i, row);
			}
		}

	}

	@Override
	public void getResult(final AppendableTable dest, final int destColumn) {
		// Preconditions.checkState(agg != null,
		// "agg should not be null. Did you call getResultSchema yet?");
		// agg.getResult(dest, destColumn);

		double logNormalization = logsumexp(partialResponsibilities);

		// Loop through and output the x vector
		for (int i = 0; i < x.length; i++) {
			dest.putDouble(destColumn + i, x[i]);
		}

		// Loop through the responsiblities and output them
		for (int i = 0; i < partialResponsibilities.length; i++) {
			dest.putDouble(destColumn + i + numDimensions,
					Math.exp(partialResponsibilities[i] - logNormalization));
		}

		// dest.putDouble(destColumn, sum * 10);
		// dest.putDouble(destColumn + 1, sum * 10);

	}

	@Override
	public Schema getResultSchema() {
		// For whatever reason, this is inconsistent with returnschema in
		// EStepAggregate, which
		// takes precedence
		// return agg.getResultSchema();
		final ImmutableList.Builder<Type> types = ImmutableList.builder();
		final ImmutableList.Builder<String> names = ImmutableList.builder();
		int numFields = numDimensions + numComponents;
		for (int i = 0; i < numFields; i++) {
			types.add(Type.DOUBLE_TYPE);
			names.add("irrelevant" + i); // Schema names don't matter here
		}
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

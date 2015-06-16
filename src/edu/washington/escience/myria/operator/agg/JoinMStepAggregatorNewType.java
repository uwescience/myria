package edu.washington.escience.myria.operator.agg;

import java.util.Objects;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.MyriaMatrix;
import edu.washington.escience.myria.PartialState;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator.AggregationOp;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * An aggregator for a column of primitive type.
 */
public class JoinMStepAggregatorNewType implements Aggregator {

	/** Required for Java serialization. */
	private static final long serialVersionUID = 1L;

	// /**
	// * Which column of the input to aggregate over. This is the partial
	// * responsibility column.
	// */
	// A field passed in by the aggregate
	private final int column;

	/**
	 * Which matrix library to use. "jblas" or "jama"
	 */
	private final String matrixLibrary = "jama";

	// /** The actual aggregator doing the work. */
	// private final PrimitiveAggregator agg;

	/**
	 * The number of components
	 */
	private final int numComponents;

	/**
	 * 
	 */
	private final int numDimensions;

	/**
	 * The column index of the gid (Gaussian component id) in the joined
	 * point/component table. Since gid is the 0th element of the Components
	 * relation, in the PointsAndComponents relation it will be after the points
	 * columns.
	 **/
	// private final int gidColumn;

	/**
	 * Data structure holding the partial state of the Gaussian parameters to be
	 * recomputed. The incrementally added points are merged into running sums,
	 * so the state uses little memory. See the inner class for details.
	 */
	private final PartialState partialState;

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
	public JoinMStepAggregatorNewType(final Schema inputSchema,
			final int column, final AggregationOp[] aggOps, int numDimensions,
			int numComponents) {
		Objects.requireNonNull(inputSchema, "inputSchema");
		this.column = column;
		// Objects.requireNonNull(aggOps, "aggOps");
		// agg = AggUtils.allocate(inputSchema.getColumnType(column),
		// inputSchema.getColumnName(column), aggOps);

		this.numDimensions = numDimensions;
		this.numComponents = numComponents;
		// gidColumn = 1 + numDimensions + numComponents;

		partialState = new PartialState(matrixLibrary);

	}

	@Override
	public void add(final ReadableTable from) {
		// Preconditions.checkState(agg != null,
		// "agg should not be null. Did you call getResultSchema yet?");
		// agg.add(from, column);
	}

	@Override
	public void addRow(final ReadableTable from, final int row) {
		// Preconditions.checkState(agg != null,
		// "agg should not be null. Did you call getResultSchema yet?");
		// agg.add(from, column, row);

		// The gaussian id also serves as the index to the responsibility array
		// long gid = from.getLong(gidColumn, row);

		// Responsiblity indices start after the x vector and id, so 1 +
		// numDimensions
		// double r_ik = from.getDouble(1 + numDimensions + (int) gid, row);

		// double x11 = from.getDouble(1, row);
		// double x21 = from.getDouble(2, row);

		// double[][] xArray = new double[][] { { x11 }, { x21 } };

		MyriaMatrix[] partialStateThisRow = new MyriaMatrix[1 + 1 + 1 + 1];

		// Get the partial state dump
		for (int i = 0; i < partialStateThisRow.length; i++) {
			partialStateThisRow[i] = from.getMyriaMatrix(1 + i, row);
		}

		// Add the current point to the partial state
		partialState.addPartialStateDumpNewType(partialStateThisRow);

	}

	@Override
	public void getResult(final AppendableTable dest, final int destColumn) {
		// Preconditions.checkState(agg != null,
		// "agg should not be null. Did you call getResultSchema yet?");
		// agg.getResult(dest, destColumn);

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
		// double r_k = r_k_partial;
		//
		// double pi = r_k / nPoints;
		//
		// DoubleMatrix mu = mu_k_partial.dup();
		// mu = mu.div(r_k);
		//
		// DoubleMatrix sigma = sigma_k_partial.dup();
		// sigma = sigma.div(r_k);
		// sigma = sigma.sub(mu.mmul(mu.transpose()));

		partialState.computeFinalParameters();

		double partpi = partialState.getFinalAmplitude();
		double[][] muArray = partialState.getFinalMu();
		double[][] sigmaArray = partialState.getFinalSigma();

		// double pi = inputColumns.get(6).getDouble(row);
		// double mu11 = mu.get(0, 0);
		// double mu21 = mu.get(1, 0);
		// double cov11 = sigma.get(0, 0);
		// double cov12 = sigma.get(0, 1);
		// double cov21 = sigma.get(1, 0);
		// double cov22 = sigma.get(1, 1);

		// double mu11 = muArray[0][0];
		// double mu21 = muArray[1][0];
		// double cov11 = sigmaArray[0][0];
		// double cov12 = sigmaArray[0][1];
		// double cov21 = sigmaArray[1][0];
		// double cov22 = sigmaArray[1][1];

		int indexCounter = 0;

		dest.putDouble(destColumn + indexCounter, partpi);
		indexCounter++;

		for (int i = 0; i < numDimensions; i++) {
			dest.putDouble(destColumn + indexCounter, muArray[i][0]);
			indexCounter++;
		}
		// dest.putDouble(destColumn + 1, mu11);
		// dest.putDouble(destColumn + 2, mu21);

		for (int i = 0; i < numDimensions; i++) {
			for (int j = 0; j < numDimensions; j++) {
				dest.putDouble(destColumn + indexCounter, sigmaArray[i][j]);
				indexCounter++;
			}
		}

		// dest.putDouble(destColumn + 3, cov11);
		// dest.putDouble(destColumn + 4, cov12);
		// dest.putDouble(destColumn + 5, cov21);
		// dest.putDouble(destColumn + 6, cov22);

	}

	@Override
	public Schema getResultSchema() {
		// For whatever reason, this is inconsistent with returnschema in
		// EStepAggregate, which
		// takes precedence
		// return agg.getResultSchema();
		final ImmutableList.Builder<Type> types = ImmutableList.builder();
		final ImmutableList.Builder<String> names = ImmutableList.builder();

		// The number of field to add to the schema, not including the group by
		// field.
		// 1 for the amplitude, nDim for the mean, ndim^2 for the cov matrix
		int numFields = 1 + numDimensions + numDimensions * numDimensions;
		// plus 1 for int type
		for (int i = 0; i < numFields + 1; i++) {
			types.add(Type.DOUBLE_TYPE);
			names.add("irrelevant" + i); // Schema names don't matter here
		}

		return new Schema(types, names);
	}

}

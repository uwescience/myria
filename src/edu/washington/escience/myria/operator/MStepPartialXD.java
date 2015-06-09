package edu.washington.escience.myria.operator;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaMatrix;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.MyriaArrayUtils;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntProcedure;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jblas.DoubleMatrix;

import Jama.Matrix;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * This is an implementation of unbalanced hash join. This operator only builds
 * hash tables for its right child, thus will begin to output tuples after right
 * child EOS.
 * 
 */
public final class MStepPartialXD extends BinaryOperator {
	/** Required for Java serialization. */
	private static final long serialVersionUID = 1L;

	/**
	 * Create logger for info logging below.
	 */
	private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory
			.getLogger(ApplyEStep.class);

	// Only used for debugging
	// private int tuples_added;

	/**
	 * The names of the output columns.
	 */
	private final ImmutableList<String> outputColumns;

	/**
	 * The column indices for comparing of child 1.
	 */
	private final int[] leftCompareIndx;
	/**
	 * The column indices for comparing of child 2.
	 */
	private final int[] rightCompareIndx;

	/**
	 * A hash table for tuples from child 2. {Hashcode -> List of tuple indices
	 * with the same hash code}
	 */
	private transient TIntObjectMap<TIntList> rightHashTableIndices;

	/**
	 * The buffer holding the valid tuples from right.
	 */
	private transient MutableTupleBuffer rightHashTable;
	/**
	 * The buffer holding the results.
	 */
	private transient TupleBatchBuffer ans;
	/** Which columns in the left child are to be output. */
	private final int[] leftAnswerColumns;
	/** Which columns in the right child are to be output. */
	private final int[] rightAnswerColumns;

	private final int numDimensions = 4;
	private final int numComponents = 7;

	Map<Integer, Double> pis;
	Map<Integer, double[][]> mus;
	Map<Integer, double[][]> sigmas;

	private boolean hasOutputAll;

	/**
	 * Contains partial state of all components for these points
	 * 
	 */
	private final PartialState[] states;

	/**
	 * Which matrix library to use. "jblas" or "jama"
	 */
	private final String matrixLibrary = "jama";

	/**
	 * Traverse through the list of tuples with the same hash code.
	 * */
	private final class JoinProcedure implements TIntProcedure {

		/**
		 * Hash table.
		 * */
		private MutableTupleBuffer joinAgainstHashTable;

		/**
     * 
     * */
		private int[] inputCmpColumns;

		/**
		 * the columns to compare against.
		 * */
		private int[] joinAgainstCmpColumns;
		/**
		 * row index of the tuple.
		 * */
		private int row;

		/**
		 * input TupleBatch.
		 * */
		private TupleBatch inputTB;

		@Override
		public boolean execute(final int index) {
			if (TupleUtils.tupleEquals(inputTB, inputCmpColumns, row,
					joinAgainstHashTable, joinAgainstCmpColumns, index)) {
				// addToAns(inputTB, row, joinAgainstHashTable, index);
			}
			return true;
		}
	};

	/**
	 * Traverse through the list of tuples.
	 * */
	private transient JoinProcedure doJoin;

	/**
	 * Construct an EquiJoin operator. It returns all columns from both children
	 * when the corresponding columns in compareIndx1 and compareIndx2 match.
	 * 
	 * @param left
	 *            the left child.
	 * @param right
	 *            the right child.
	 * @param compareIndx1
	 *            the columns of the left child to be compared with the right.
	 *            Order matters.
	 * @param compareIndx2
	 *            the columns of the right child to be compared with the left.
	 *            Order matters.
	 * @throw IllegalArgumentException if there are duplicated column names from
	 *        the children.
	 */
	public MStepPartialXD(final Operator left, final Operator right,
			final int[] compareIndx1, final int[] compareIndx2) {
		this(null, left, right, compareIndx1, compareIndx2);
	}

	/**
	 * Construct an EquiJoin operator. It returns the specified columns from
	 * both children when the corresponding columns in compareIndx1 and
	 * compareIndx2 match.
	 * 
	 * @param left
	 *            the left child.
	 * @param right
	 *            the right child.
	 * @param compareIndx1
	 *            the columns of the left child to be compared with the right.
	 *            Order matters.
	 * @param compareIndx2
	 *            the columns of the right child to be compared with the left.
	 *            Order matters.
	 * @param answerColumns1
	 *            the columns of the left child to be returned. Order matters.
	 * @param answerColumns2
	 *            the columns of the right child to be returned. Order matters.
	 * @throw IllegalArgumentException if there are duplicated column names in
	 *        <tt>outputSchema</tt>, or if <tt>outputSchema</tt> does not have
	 *        the correct number of columns and column types.
	 */
	public MStepPartialXD(final Operator left, final Operator right,
			final int[] compareIndx1, final int[] compareIndx2,
			final int[] answerColumns1, final int[] answerColumns2) {
		this(null, left, right, compareIndx1, compareIndx2, answerColumns1,
				answerColumns2);
	}

	/**
	 * Main constructor
	 * 
	 * 
	 * 
	 * Construct an EquiJoin operator. It returns the specified columns from
	 * both children when the corresponding columns in compareIndx1 and
	 * compareIndx2 match.
	 * 
	 * @param outputColumns
	 *            the names of the columns in the output schema. If null, the
	 *            corresponding columns will be copied from the children.
	 * @param left
	 *            the left child.
	 * @param right
	 *            the right child.
	 * @param compareIndx1
	 *            the columns of the left child to be compared with the right.
	 *            Order matters.
	 * @param compareIndx2
	 *            the columns of the right child to be compared with the left.
	 *            Order matters.
	 * @param answerColumns1
	 *            the columns of the left child to be returned. Order matters.
	 * @param answerColumns2
	 *            the columns of the right child to be returned. Order matters.
	 * @throw IllegalArgumentException if there are duplicated column names in
	 *        <tt>outputColumns</tt>, or if <tt>outputColumns</tt> does not have
	 *        the correct number of columns and column types.
	 */
	public MStepPartialXD(final List<String> outputColumns,
			final Operator left, final Operator right,
			final int[] compareIndx1, final int[] compareIndx2,
			final int[] answerColumns1, final int[] answerColumns2) {
		super(left, right);
		Preconditions.checkArgument(compareIndx1.length == compareIndx2.length);
		if (outputColumns != null) {
			Preconditions
					.checkArgument(
							outputColumns.size() == answerColumns1.length
									+ answerColumns2.length,
							"length mismatch between output column names and columns selected for output");
			Preconditions.checkArgument(ImmutableSet.copyOf(outputColumns)
					.size() == outputColumns.size(),
					"duplicate column names in outputColumns");
			this.outputColumns = ImmutableList.copyOf(outputColumns);
		} else {
			this.outputColumns = null;
		}
		leftCompareIndx = MyriaArrayUtils.warnIfNotSet(compareIndx1);
		rightCompareIndx = MyriaArrayUtils.warnIfNotSet(compareIndx2);
		leftAnswerColumns = MyriaArrayUtils.warnIfNotSet(answerColumns1);
		rightAnswerColumns = MyriaArrayUtils.warnIfNotSet(answerColumns2);
		pis = new HashMap<Integer, Double>();
		mus = new HashMap<Integer, double[][]>();
		sigmas = new HashMap<Integer, double[][]>();
		hasOutputAll = false;
		states = new PartialState[numComponents];
		for (int i = 0; i < numComponents; i++) {
			states[i] = new PartialState(matrixLibrary);
		}
		// tuples_added = 0;
	}

	/**
	 * Construct an EquiJoin operator. It returns all columns from both children
	 * when the corresponding columns in compareIndx1 and compareIndx2 match.
	 * 
	 * @param outputColumns
	 *            the names of the columns in the output schema. If null, the
	 *            corresponding columns will be copied from the children.
	 * @param left
	 *            the left child.
	 * @param right
	 *            the right child.
	 * @param compareIndx1
	 *            the columns of the left child to be compared with the right.
	 *            Order matters.
	 * @param compareIndx2
	 *            the columns of the right child to be compared with the left.
	 *            Order matters.
	 * @throw IllegalArgumentException if there are duplicated column names in
	 *        <tt>outputSchema</tt>, or if <tt>outputSchema</tt> does not have
	 *        the correct number of columns and column types.
	 */
	public MStepPartialXD(final List<String> outputColumns,
			final Operator left, final Operator right,
			final int[] compareIndx1, final int[] compareIndx2) {
		this(outputColumns, left, right, compareIndx1, compareIndx2, range(left
				.getSchema().numColumns()), range(right.getSchema()
				.numColumns()));
	}

	/**
	 * Helper function that generates an array of the numbers 0..max-1.
	 * 
	 * @param max
	 *            the size of the array.
	 * @return an array of the numbers 0..max-1.
	 */
	private static int[] range(final int max) {
		int[] ret = new int[max];
		for (int i = 0; i < max; ++i) {
			ret[i] = i;
		}
		return ret;
	}

	@Override
	protected Schema generateSchema() {
		final Schema leftSchema = getLeft().getSchema();
		final Schema rightSchema = getRight().getSchema();
		ImmutableList.Builder<Type> types = ImmutableList.builder();
		ImmutableList.Builder<String> names = ImmutableList.builder();

		/* Assert that the compare index types are the same. */
		for (int i = 0; i < rightCompareIndx.length; ++i) {
			int leftIndex = leftCompareIndx[i];
			int rightIndex = rightCompareIndx[i];
			Type leftType = leftSchema.getColumnType(leftIndex);
			Type rightType = rightSchema.getColumnType(rightIndex);
			Preconditions
					.checkState(
							leftType == rightType,
							"column types do not match for join at index %s: left column type %s [%s] != right column type %s [%s]",
							i, leftIndex, leftType, rightIndex, rightType);
		}

		// for (int i : leftAnswerColumns) {
		// types.add(leftSchema.getColumnType(i));
		// names.add(leftSchema.getColumnName(i));
		// }

		// for (int i : rightAnswerColumns) {
		// types.add(rightSchema.getColumnType(i));
		// names.add(rightSchema.getColumnName(i));
		// }

		types.add(Type.LONG_TYPE);
		names.add("gid");

		for (int i = 0; i < 1 + 1 + numDimensions + numDimensions
				* numDimensions; i++) {
			types.add(Type.DOUBLE_TYPE);
			names.add("col" + i);
		}

		if (outputColumns != null) {
			return new Schema(types.build(), outputColumns);
		} else {
			return new Schema(types, names);
		}
	}

	/**
	 * @param cntTB
	 *            current TB
	 * @param row
	 *            current row
	 * @param hashTable
	 *            the buffer holding the tuples to join against
	 * @param index
	 *            the index of hashTable, which the cntTuple is to join with
	 */
	protected void addToAns(final TupleBatch cntTB, final int row,
			final MutableTupleBuffer hashTable, final int index) {
		List<? extends Column<?>> tbColumns = cntTB.getDataColumns();
		ReadableColumn[] hashTblColumns = hashTable.getColumns(index);
		int tupleIdx = hashTable.getTupleIndexInContainingTB(index);

		// for (int i = 0; i < leftAnswerColumns.length; ++i) {
		// ans.put(i, tbColumns.get(leftAnswerColumns[i]), row);
		// }

		// for (int i = 0; i < rightAnswerColumns.length; ++i) {
		// ans.put(i + leftAnswerColumns.length,
		// hashTblColumns[rightAnswerColumns[i]], tupleIdx);
		// }

	}

	@Override
	protected void cleanup() throws DbException {
		rightHashTable = null;
		rightHashTableIndices = null;
		ans = null;
	}

	@Override
	public void checkEOSAndEOI() {
		final Operator left = getLeft();
		final Operator right = getRight();

		if (left.eos() && right.eos() && ans.numTuples() == 0) {
			setEOS();
			return;
		}

		// EOS could be used as an EOI
		if ((childrenEOI[0] || left.eos()) && (childrenEOI[1] || right.eos())
				&& ans.numTuples() == 0) {
			setEOI(true);
			Arrays.fill(childrenEOI, false);
		}
	}

	/**
	 * Recording the EOI status of the children.
	 */
	private final boolean[] childrenEOI = new boolean[2];

	/**
	 * Note: If this operator is ready for EOS, this function will return true
	 * since EOS is a special EOI.
	 * 
	 * @return whether this operator is ready to set itself EOI
	 */
	private boolean isEOIReady() {
		if ((childrenEOI[0] || getLeft().eos())
				&& (childrenEOI[1] || getRight().eos())) {
			return true;
		}
		return false;
	}

	@Override
	protected TupleBatch fetchNextReady() throws DbException {
		/*
		 * blocking mode will have the same logic
		 */

		/* If any full tuple batches are ready, output them. */
		TupleBatch nexttb = ans.popAnyUsingTimeout();
		if (nexttb != null) {
			return nexttb;
		}

		final Operator right = getRight();

		/* Drain the right child. */
		while (!right.eos()) {
			TupleBatch rightTB = right.nextReady();
			if (rightTB == null) {
				/*
				 * The right child may have realized it's EOS now. If so, we
				 * must move onto left child to avoid livelock.
				 */
				if (right.eos()) {
					break;
				}
				return null;
			}
			processRightChildTB(rightTB);
		}

		/* The right child is done, let's drain the left child. */
		final Operator left = getLeft();
		while (!left.eos()) {
			TupleBatch leftTB = left.nextReady();
			/*
			 * Left tuple has no data, but we may need to pop partially-full
			 * existing batches if left reached EOI/EOS. Break and check for
			 * termination.
			 */
			if (leftTB == null) {
				break;
			}

			/* Process the data and add new results to ans. */
			processLeftChildTB(leftTB);

			nexttb = ans.popAnyUsingTimeout();
			if (nexttb != null) {
				return nexttb;
			}
			/*
			 * We didn't time out or there is no data in ans, and there are no
			 * full tuple batches. Either way, check for more data.
			 */
		}

		if (isEOIReady()) {
			/*
			 * New JoinMStep logic: Left child has reached EOS, so we now can
			 * output partial responsibilities. ans.putLong(0, 0); nexttb =
			 * ans.popAny();
			 */
			if (!hasOutputAll) {
				// Add some tuples to the output
				for (int k = 0; k < numComponents; k++) {
					// add the gaussian id
					ans.putLong(0, k);
					double[] partialState = states[k].getPartialStateDump();
					for (int i = 0; i < partialState.length; i++) {
						ans.putDouble(i + 1, partialState[i]);
					}

					// for testing:
					// PartialState newState = new PartialState(matrixLibrary);
					// newState.addPartialStateDump(states[k].getPartialStateDump());
					// double[] partialState = newState.getPartialStateDump();
					//
					// for (int i = 0; i < partialState.length; i++) {
					// ans.putDouble(i + 1, partialState[i]);
					// }

				}
				hasOutputAll = true;
				// LOGGER.info("NUMBER OF TUPLES PROCESSESED WHEN COMPUTING OUTPUT = "
				// + tuples_added);
				// LOGGER.info("childrenEOI[0]  left.eos()  childrenEOI[1]  right.eos()");
				// LOGGER.info(String.valueOf(childrenEOI[0]) + "  "
				// + String.valueOf(left.eos()) + "  "
				// + String.valueOf(childrenEOI[1]) + "  "
				// + String.valueOf(right.eos()));
			}

			nexttb = ans.popAny();
		}

		return nexttb;
	}

	@Override
	public void init(final ImmutableMap<String, Object> execEnvVars)
			throws DbException {
		final Operator right = getRight();

		rightHashTableIndices = new TIntObjectHashMap<TIntList>();
		rightHashTable = new MutableTupleBuffer(right.getSchema());

		ans = new TupleBatchBuffer(getSchema());
		doJoin = new JoinProcedure();
	}

	/**
	 * Process the tuples from left child.
	 * 
	 * The left child is the points relation, so for each point we will add it
	 * to the partial state of each Gaussian component.
	 * 
	 * @param tb
	 *            TupleBatch to be processed.
	 */
	protected void processLeftChildTB(final TupleBatch tb) {
		doJoin.joinAgainstHashTable = rightHashTable;
		doJoin.inputCmpColumns = leftCompareIndx;
		doJoin.joinAgainstCmpColumns = rightCompareIndx;
		doJoin.inputTB = tb;

		List<? extends Column<?>> inputColumns = tb.getDataColumns();

		for (int row = 0; row < tb.numTuples(); ++row) {
			// indexCounter keeps track of the column index being scanned in
			// from the current tuple.
			int indexCounter = 0;

			long pid = inputColumns.get(indexCounter).getLong(row);
			indexCounter++;

			double[][] xArray = new double[numDimensions][1];
			MyriaMatrix xMatrix = inputColumns.get(indexCounter)
					.getMyriaMatrix(row);
			indexCounter++;

			xArray = xMatrix.getArray();

			double[] responsibilities = new double[numComponents];
			MyriaMatrix respMatrix = inputColumns.get(indexCounter)
					.getMyriaMatrix(row);
			indexCounter++;
			double[][] respArray = respMatrix.getArray();

			for (int i = 0; i < numComponents; i++) {
				// responsibilities[i] =
				// inputColumns.get(indexCounter).getDouble(
				// row);
				responsibilities[i] = respArray[i][0];
			}

			List<MyriaMatrix> bs = new ArrayList<MyriaMatrix>();
			// Get the Matrices for b
			for (int i = 0; i < numComponents; i++) {
				MyriaMatrix b_j = inputColumns.get(indexCounter)
						.getMyriaMatrix(row);
				bs.add(b_j);
				indexCounter++;
			}

			List<MyriaMatrix> Bs = new ArrayList<MyriaMatrix>();
			// Get the Matrices for B
			for (int i = 0; i < numComponents; i++) {
				MyriaMatrix bSigma_j = inputColumns.get(indexCounter)
						.getMyriaMatrix(row);
				Bs.add(bSigma_j);
				indexCounter++;
			}

			// For each component, add the current point to that component
			// with the correct responsibility.
			for (int i = 0; i < numComponents; i++) {
				states[i].addPoint(xArray, responsibilities[i]);
			}
		}
	}

	/**
	 * Process the tuples from right child.
	 * 
	 * @param tb
	 *            TupleBatch to be processed.
	 */
	protected void processRightChildTB(final TupleBatch tb) {
		// For the MStep, we don't need the parameters of the Gaussians, so this
		// is really a pure aggregate operator rather than a join. This will be
		// reworked later.
	}

	/**
	 * @param tb
	 *            the source TupleBatch
	 * @param row
	 *            the row number to get added to hash table
	 * @param hashTable
	 *            the target hash table
	 * @param hashTable1IndicesLocal
	 *            hash table 1 indices local
	 * @param hashCode
	 *            the hashCode of the tb.
	 * */
	private void addToHashTable(final TupleBatch tb, final int row,
			final MutableTupleBuffer hashTable,
			final TIntObjectMap<TIntList> hashTable1IndicesLocal,
			final int hashCode) {
		final int nextIndex = hashTable.numTuples();
		TIntList tupleIndicesList = hashTable1IndicesLocal.get(hashCode);
		if (tupleIndicesList == null) {
			tupleIndicesList = new TIntArrayList(1);
			hashTable1IndicesLocal.put(hashCode, tupleIndicesList);
		}
		tupleIndicesList.add(nextIndex);
		List<? extends Column<?>> inputColumns = tb.getDataColumns();
		for (int column = 0; column < tb.numColumns(); column++) {
			hashTable.put(column, inputColumns.get(column), row);
		}
	}

	private class PartialState implements Serializable {

		/** Required for Java serialization. */
		private static final long serialVersionUID = 1L;

		/**
		 * The Matrix library we'll use for matrix operators
		 */
		private final String matrixLibrary;

		/**
		 * The partial sum of responsibilities for the points encountered so
		 * far.
		 */
		private double r_k_partial;

		/**
		 * The partial sum of means over the points encountered so far. This is
		 * stored in the jblas DoubleMatrix class.
		 */
		private DoubleMatrix mu_k_partial;

		/**
		 * The partial sum of covariances over the points encountered so far.
		 * This is stored in the jblas DoubleMatrix class.
		 */
		private DoubleMatrix sigma_k_partial;

		/**
		 * The partial sum of means over the points encountered so far. This is
		 * stored in the JAMA Matrix class.
		 */
		private Matrix jama_mu_k_partial;

		/**
		 * The partial sum of covariances over the points encountered so far.
		 * This is stored in the JAMA Matrix class.
		 */
		private Matrix jama_sigma_k_partial;

		/**
		 * The number of points encountered so far.
		 */
		private int nPoints;

		/**
		 * A boolean flag indicating whether the final parameters have been
		 * computed or not.
		 */
		private boolean isCompleted;

		/**
		 * The final sum of responsibilities for this Gaussian component.
		 */
		private double r_k;

		/**
		 * The final amplitude of this Gaussian component.
		 */
		private double piFinal;

		/**
		 * The final mean vector of this Gaussian components.
		 */
		private double[][] muFinalArray;

		/**
		 * The final covariance matrix of this Gaussian component.
		 */
		private double[][] sigmaFinalArray;

		/**
		 * Constructs a partial state
		 */
		private PartialState(String matrixLibrary) {
			this.matrixLibrary = matrixLibrary;

			// Initialize running parameters
			r_k_partial = 0.0;

			// jblas
			mu_k_partial = DoubleMatrix.zeros(numDimensions, 1);
			sigma_k_partial = DoubleMatrix.zeros(numDimensions, numDimensions);

			// jama
			double[][] zeroVec = new double[numDimensions][1];
			double[][] zeroMatrix = new double[numDimensions][numDimensions];
			jama_mu_k_partial = new Matrix(zeroVec);
			jama_sigma_k_partial = new Matrix(zeroMatrix);

			isCompleted = false;
		}

		private void addPoint(double[][] xArray, double r_ik) {
			nPoints += 1;

			//
			// // Do updates
			r_k_partial += r_ik;

			if (matrixLibrary.equals("jblas")) {
				DoubleMatrix x = new DoubleMatrix(xArray);
				mu_k_partial = mu_k_partial.add(x.mul(r_ik));
				sigma_k_partial = sigma_k_partial.add(x.mmul(x.transpose())
						.mul(r_ik));
			} else if (matrixLibrary.equals("jama")) {
				Matrix x = new Matrix(xArray);
				jama_mu_k_partial = jama_mu_k_partial.plus(x.times(r_ik));
				jama_sigma_k_partial = jama_sigma_k_partial.plus(x.times(
						x.transpose()).times(r_ik));
			} else {
				throw new RuntimeException("Incorrect matrix libary specified.");
			}

		}

		// ONLY FOR JAMA
		private double[] getPartialStateDump() {
			double[] partialState = new double[1 + 1 + numDimensions
					+ numDimensions * numDimensions];

			double partNPoints = nPoints;
			double partRK = r_k_partial;
			double[][] muPart = jama_mu_k_partial.getArrayCopy();
			double[][] sigmaPart = jama_sigma_k_partial.getArrayCopy();

			// Fill the partial state array
			int indexCounter = 0;

			partialState[indexCounter] = partNPoints;
			indexCounter++;

			partialState[indexCounter] = partRK;
			indexCounter++;

			for (int i = 0; i < numDimensions; i++) {
				partialState[indexCounter] = muPart[i][0];
				indexCounter++;
			}

			for (int i = 0; i < numDimensions; i++) {
				for (int j = 0; j < numDimensions; j++) {
					partialState[indexCounter] = sigmaPart[i][j];
					indexCounter++;
				}
			}

			return partialState;
		}

		// ONLY FOR JAMA
		private void addPartialStateDump(double[] partialStateDump) {

			// Build the data structures we need
			double partNPoints;
			double partRK;
			double[][] muPart = new double[numDimensions][1];
			double[][] sigmaPart = new double[numDimensions][numDimensions];

			// Convert from dump to parameters
			int indexCounter = 0;
			partNPoints = partialStateDump[indexCounter];
			indexCounter++;
			partRK = partialStateDump[indexCounter];
			indexCounter++;
			for (int i = 0; i < numDimensions; i++) {
				muPart[i][0] = partialStateDump[indexCounter];
				indexCounter++;
			}
			for (int i = 0; i < numDimensions; i++) {
				for (int j = 0; j < numDimensions; j++) {
					sigmaPart[i][j] = partialStateDump[indexCounter];
					indexCounter++;
				}
			}

			// Create matrices and add them to the current state
			Matrix jama_mu_k_from_dump = new Matrix(muPart);
			Matrix jama_sigma_k_from_dump = new Matrix(sigmaPart);

			nPoints += partNPoints;
			r_k_partial += partRK;
			jama_mu_k_partial = jama_mu_k_partial.plus(jama_mu_k_from_dump);
			jama_sigma_k_partial = jama_sigma_k_partial
					.plus(jama_sigma_k_from_dump);
		}

		/**
		 * 
		 * @return double amplitude
		 */
		private double getFinalAmplitude() {
			return r_k / nPoints;
		}

		/**
		 * 
		 * @return double array of the vector representation
		 */
		private double[][] getFinalMu() {
			return muFinalArray;
		}

		/**
		 * 
		 * @return double array of the matrix representation
		 */
		private double[][] getFinalSigma() {
			return sigmaFinalArray;
		}

		private void computeFinalParameters() {
			if (isCompleted) {
				throw new RuntimeException(
						"Called computeFinalParameters when already computed.");
			}

			// Finalize r_k
			r_k = r_k_partial;

			if (matrixLibrary.equals("jblas")) {
				// Finalize mu
				DoubleMatrix mu = mu_k_partial.dup();
				mu = mu.div(r_k);

				// Finalize sigma
				DoubleMatrix sigma = sigma_k_partial.dup();
				sigma = sigma.div(r_k);
				sigma = sigma.sub(mu.mmul(mu.transpose()));

				muFinalArray = mu.toArray2();
				sigmaFinalArray = sigma.toArray2();
			} else if (matrixLibrary.equals("jama")) {
				// Finalize mu
				Matrix mu = jama_mu_k_partial.copy();
				mu = mu.times(1. / r_k);

				// Finialize sigma
				Matrix sigma = jama_sigma_k_partial.copy();
				sigma = sigma.times(1. / r_k);
				sigma = sigma.minus(mu.times(mu.transpose()));

				muFinalArray = mu.getArrayCopy();
				sigmaFinalArray = sigma.getArrayCopy();
			} else {
				throw new RuntimeException("Incorrect matrix libary specified.");
			}

			isCompleted = true;
		}
	}

}

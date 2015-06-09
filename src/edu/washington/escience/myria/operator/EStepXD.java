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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jblas.Decompose;
import org.jblas.DoubleMatrix;
import org.jblas.Solve;

import Jama.Matrix;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Implements the EStep in an Extreme Deconvolution.
 * 
 * This is an implementation of unbalanced hash join. This operator only builds
 * hash tables for its right child, thus will begin to output tuples after right
 * child EOS.
 * 
 */
public final class EStepXD extends BinaryOperator {
	/** Required for Java serialization. */
	private static final long serialVersionUID = 1L;

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
	public EStepXD(final Operator left, final Operator right,
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
	public EStepXD(final Operator left, final Operator right,
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
	public EStepXD(final List<String> outputColumns, final Operator left,
			final Operator right, final int[] compareIndx1,
			final int[] compareIndx2, final int[] answerColumns1,
			final int[] answerColumns2) {
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
	public EStepXD(final List<String> outputColumns, final Operator left,
			final Operator right, final int[] compareIndx1,
			final int[] compareIndx2) {
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

		// Now using my own type
		// for (int i : leftAnswerColumns) {
		// types.add(leftSchema.getColumnType(i));
		// names.add(leftSchema.getColumnName(i));
		// }

		// New Matrix type in schema
		types.add(Type.LONG_TYPE);
		names.add("pid");

		// New Matrix type in schema
		types.add(Type.MYRIAMATRIX_TYPE);
		names.add("x");

		// New Matrix type in schema
		types.add(Type.MYRIAMATRIX_TYPE);
		names.add("responsibilities");

		// The conditional means b
		for (int i = 0; i < numComponents; i++) {
			// New Matrix type in schema
			types.add(Type.MYRIAMATRIX_TYPE);
			names.add("b" + (i + 1));
		}

		// The conditional sigmas B
		for (int i = 0; i < numComponents; i++) {
			// New Matrix type in schema
			types.add(Type.MYRIAMATRIX_TYPE);
			names.add("B" + (i + 1));
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
	 * @param tb
	 *            TupleBatch to be processed.
	 */
	protected void processLeftChildTB(final TupleBatch tb) {
		doJoin.joinAgainstHashTable = rightHashTable;
		doJoin.inputCmpColumns = leftCompareIndx;
		doJoin.joinAgainstCmpColumns = rightCompareIndx;
		doJoin.inputTB = tb;

		// ans.absorb(tb);
		List<? extends Column<?>> inputColumns = tb.getDataColumns();

		for (int row = 0; row < tb.numTuples(); ++row) {

			int inputIndexCounter = 0;
			int outputIndexCounter = 0;

			long pid = inputColumns.get(inputIndexCounter).getLong(row);
			inputIndexCounter++;

			ans.putLong(outputIndexCounter, pid);
			outputIndexCounter++;

			// Read the x array
			double[][] xArray = new double[numDimensions][1];
			for (int i = 0; i < numDimensions; i++) {
				xArray[i][0] = inputColumns.get(inputIndexCounter).getDouble(
						row);
				inputIndexCounter++;
			}

			// Read the array of X error values, stored in row-major order
			double[][] sigmaArray = new double[numDimensions][numDimensions];
			for (int i = 0; i < numDimensions; i++) {
				for (int j = 0; j < numDimensions; j++) {
					sigmaArray[i][j] = inputColumns.get(inputIndexCounter)
							.getDouble(row);
					inputIndexCounter++;
				}
			}

			Matrix x = new Matrix(xArray);
			Matrix xSigma = new Matrix(sigmaArray);

			// Output x matrix
			ans.putMyriaMatrix(outputIndexCounter, new MyriaMatrix(xArray));
			outputIndexCounter++;

			// Responsibilities q
			// TODO use T instead of sigmas.get(i)
			// Calculate the partial responsibilities
			double[] partialResponsibilities = new double[numComponents];
			for (int i = 0; i < numComponents; i++) {
				// Modification for XD, get partial responsibilities with T
				// instead of V
				// //Matrix sigma_j = new Matrix(sigmas.get(i));
				// //Matrix T = xSigma.plus(sigma_j);
				// Plug T into the gaussian evaluator T.getarray()
				partialResponsibilities[i] = getPartialResponsibility(xArray,
						pis.get(i), mus.get(i), sigmas.get(i));
			}

			double logNormalization = logsumexp(partialResponsibilities);

			// for (int i = 0; i < numComponents; i++) {
			// ans.putDouble(inputIndexCounter,
			// Math.exp(partialResponsibilities[i] - logNormalization));
			// inputIndexCounter++;
			// }

			// ans.putDouble
			// final int cntHashCode = HashUtils.hashSubRow(tb,
			// doJoin.inputCmpColumns, row);
			// TIntList tuplesWithHashCode = rightHashTableIndices
			// .get(cntHashCode);
			// if (tuplesWithHashCode != null) {
			// doJoin.row = row;
			// tuplesWithHashCode.forEach(doJoin);
			// }
			double[][] respArray = new double[numComponents][1];
			for (int i = 0; i < numComponents; i++) {
				respArray[i][0] = Math.exp(partialResponsibilities[i]
						- logNormalization);
			}
			Matrix respMatrix = new Matrix(respArray);
			ans.putMyriaMatrix(outputIndexCounter, new MyriaMatrix(respMatrix));
			outputIndexCounter++;

			// Get the conditional distributions
			List<Matrix> bs = new ArrayList<Matrix>();
			List<Matrix> Bs = new ArrayList<Matrix>();

			// Vectors b
			for (int i = 0; i < numComponents; i++) {
				Matrix mu_j = new Matrix(mus.get(i));
				Matrix sigma_j = new Matrix(sigmas.get(i));

				Matrix w_m = x.minus(mu_j);
				Matrix T = xSigma.plus(sigma_j);

				Matrix Tinv = T.inverse();

				Matrix b = mu_j.plus(sigma_j.times(Tinv).times(w_m));
				Matrix B = sigma_j.minus(sigma_j.times(Tinv).times(sigma_j));

				bs.add(b);
				Bs.add(B);
			}

			// Put the b vectors in the output
			for (int i = 0; i < numComponents; i++) {
				ans.putMyriaMatrix(outputIndexCounter,
						new MyriaMatrix(bs.get(i)));
				outputIndexCounter++;
			}

			// Put the B matrices in the output
			for (int i = 0; i < numComponents; i++) {
				ans.putMyriaMatrix(outputIndexCounter,
						new MyriaMatrix(bs.get(i)));
				outputIndexCounter++;
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

		// Map<Integer, Double> pis = new HashMap<Integer, Double>();
		// Map<Integer, double[][]> mus = new HashMap<Integer, double[][]>();
		// Map<Integer, double[][]> sigmas = new HashMap<Integer, double[][]>();

		List<? extends Column<?>> inputColumns = tb.getDataColumns();

		for (int row = 0; row < tb.numTuples(); ++row) {
			// for (int column = 0; column < tb.numColumns(); ++column) {
			// LOGGER.info("Column value is: "
			// + inputColumns.get(column).getLong(row));
			// }

			// Read the data into data structures for computation
			int indexCounter = 0;

			long gid = inputColumns.get(indexCounter).getLong(row);
			indexCounter++;

			double pi = inputColumns.get(indexCounter).getDouble(row);
			indexCounter++;

			double[][] muArray = new double[numDimensions][1];
			for (int i = 0; i < numDimensions; i++) {
				muArray[i][0] = inputColumns.get(indexCounter).getDouble(row);
				indexCounter++;
			}

			// Read in sigma in row-major order
			double[][] sigmaArray = new double[numDimensions][numDimensions];
			for (int i = 0; i < numDimensions; i++) {
				for (int j = 0; j < numDimensions; j++) {
					sigmaArray[i][j] = inputColumns.get(indexCounter)
							.getDouble(row);
					indexCounter++;
				}
			}

			pis.put((int) gid, pi);
			mus.put((int) gid, muArray);
			sigmas.put((int) gid, sigmaArray);

			// final int cntHashCode = HashUtils.hashSubRow(tb,
			// rightCompareIndx,
			// row);
			// // only build hash table on two sides if none of the children is
			// EOS
			// addToHashTable(tb, row, rightHashTable, rightHashTableIndices,
			// cntHashCode);
		}

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

	private double getPartialResponsibility(double[][] xArray, double pi,
			double[][] muArray, double[][] sigmaArray) {
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
		// double[][] xArray = new double[][] { { x11 }, { x21 } };
		// double[][] sigmaArray = new double[][] { { cov11, cov12 },
		// { cov21, cov22 } };
		// double[][] muArray = new double[][] { { mu11 }, { mu21 } };

		// Compute Log of Gaussian kernal, -1/2 * (x - mu).T * V * (x - mu)
		double kernal;
		double det;
		if (matrixLibrary.equals("jblas")) {
			// Using jblas library
			kernal = getKernalJblas(xArray, muArray, sigmaArray);
			det = getDeterminantJblas(sigmaArray);
		} else if (matrixLibrary.equals("jama")) {
			// Using jama libary
			kernal = getKernalJama(xArray, muArray, sigmaArray);
			det = getDeterminantJama(sigmaArray);
		} else {
			throw new RuntimeException("Incorrect matrix libary specified.");
		}

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

	private double getKernalJama(double[][] xArray, double[][] muArray,
			double[][] sigmaArray) {
		Matrix jama_x = new Matrix(xArray);
		Matrix jama_mu = new Matrix(muArray);
		Matrix jama_Sigma = new Matrix(sigmaArray);

		Matrix jama_SigmaInv = jama_Sigma.inverse();
		Matrix jama_x_mu = jama_x.minus(jama_mu);

		return jama_x_mu.transpose().times(jama_SigmaInv.times(jama_x_mu))
				.times(-.5).get(0, 0);
	}

	// Computes determinant of Sigma
	private double getDeterminantJblas(double[][] sigmaArray) {
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
		return Math.abs(det);
	}

	private double getDeterminantJama(double[][] sigmaArray) {
		Matrix Sigma = new Matrix(sigmaArray);
		return Sigma.det();
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

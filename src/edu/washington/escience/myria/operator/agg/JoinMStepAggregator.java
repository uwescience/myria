package edu.washington.escience.myria.operator.agg;

import java.io.Serializable;
import java.util.Objects;

import org.jblas.DoubleMatrix;

import com.google.common.collect.ImmutableList;

import Jama.Matrix;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator.AggregationOp;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * An aggregator for a column of primitive type.
 */
public class JoinMStepAggregator implements Aggregator {

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
   * The column index of the gid (Gaussian component id) in the joined point/component table. Since
   * gid is the 0th element of the Components relation, in the PointsAndComponents relation it will
   * be after the points columns.
   **/
  // private final int gidColumn;

  /**
   * Data structure holding the partial state of the Gaussian parameters to be recomputed. The
   * incrementally added points are merged into running sums, so the state uses little memory. See
   * the inner class for details.
   */
  private final PartialState partialState;

  /**
   * A wrapper for the {@link PrimitiveAggregator} implementations like {@link IntegerAggregator}.
   * 
   * @param inputSchema the schema of the input tuples.
   * @param column which column of the input to aggregate over.
   * @param aggOps which aggregate operations are requested. See {@link PrimitiveAggregator}.
   */
  public JoinMStepAggregator(final Schema inputSchema, final int column,
      final AggregationOp[] aggOps, final int numDimensions, final int numComponents) {
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
  public void add(final ReadableTable from, final Object state) {
    // Preconditions.checkState(agg != null,
    // "agg should not be null. Did you call getResultSchema yet?");
    // agg.add(from, column);
  }

  @Override
  public void addRow(final ReadableTable from, final int row, final Object state) {
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

    double[] partialStateThisRow =
        new double[1 + 1 + numDimensions + numDimensions * numDimensions];

    // Get the partial state dump
    for (int i = 0; i < partialStateThisRow.length; i++) {
      partialStateThisRow[i] = from.getDouble(1 + i, row);
    }

    // Add the current point to the partial state
    partialState.addPartialStateDump(partialStateThisRow);

  }

  @Override
  public void getResult(final AppendableTable dest, final int destColumn, final Object state) {
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
    for (int i = 0; i < numFields; i++) {
      types.add(Type.DOUBLE_TYPE);
      names.add("irrelevant" + i); // Schema names don't matter here
    }

    return new Schema(types, names);
  }

  private class PartialState implements Serializable {

    /** Required for Java serialization. */
    private static final long serialVersionUID = 1L;

    /**
     * 
     */
    private final String matrixLibrary;

    /**
     * 
     */
    private double r_k_partial;

    /**
     * 
     */
    private DoubleMatrix mu_k_partial;

    /**
     * 
     */
    private DoubleMatrix sigma_k_partial;

    /**
     * 
     */
    private Matrix jama_mu_k_partial;

    /**
     * 
     */
    private Matrix jama_sigma_k_partial;

    /**
     * 
     */
    private int nPoints;

    /**
     * 
     */
    private boolean isCompleted;

    /**
     * 
     */
    private double r_k;

    /**
     * 
     */
    private double piFinal;

    /**
     * 
     */
    private double[][] muFinalArray;

    /**
     * 
     */
    private double[][] sigmaFinalArray;

    /**
     * 
     */
    private PartialState(final String matrixLibrary) {
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

    private void addPoint(final double[][] xArray, final double r_ik) {
      nPoints += 1;

      //
      // // Do updates
      r_k_partial += r_ik;

      if (matrixLibrary.equals("jblas")) {
        DoubleMatrix x = new DoubleMatrix(xArray);
        mu_k_partial = mu_k_partial.add(x.mul(r_ik));
        sigma_k_partial = sigma_k_partial.add(x.mmul(x.transpose()).mul(r_ik));
      } else if (matrixLibrary.equals("jama")) {
        Matrix x = new Matrix(xArray);
        jama_mu_k_partial = jama_mu_k_partial.plus(x.times(r_ik));
        jama_sigma_k_partial = jama_sigma_k_partial.plus(x.times(x.transpose()).times(r_ik));
      } else {
        throw new RuntimeException("Incorrect matrix libary specified.");
      }

    }

    // ONLY FOR JAMA
    private double[] getPartialStateDump() {
      double[] partialState = new double[1 + 1 + numDimensions + numDimensions * numDimensions];

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
    private void addPartialStateDump(final double[] partialStateDump) {

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
      jama_sigma_k_partial = jama_sigma_k_partial.plus(jama_sigma_k_from_dump);
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
        throw new RuntimeException("Called computeFinalParameters when already computed.");
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

  /*
   * (non-Javadoc)
   * 
   * @see edu.washington.escience.myria.operator.agg.Aggregator#getInitialState()
   */
  @Override
  public Object getInitialState() {
    // TODO Auto-generated method stub
    return null;
  }
}

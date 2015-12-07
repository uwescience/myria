/**
 *
 */
package edu.washington.escience.myria;

import java.io.Serializable;

import org.jblas.DoubleMatrix;

import Jama.Matrix;

/**
 * 
 */

public class PartialState implements Serializable {

	/** Required for Java serialization. */
	private static final long serialVersionUID = 1L;

	/**
	 * The number of dimensions
	 */
	private final int numDimensions = 4;

	/**
	 * The Matrix library we'll use for matrix operators
	 */
	private final String matrixLibrary;

	/**
	 * The partial sum of responsibilities for the points encountered so far.
	 */
	private double r_k_partial;

	/**
	 * The partial sum of means over the points encountered so far. This is
	 * stored in the jblas DoubleMatrix class.
	 */
	private DoubleMatrix mu_k_partial;

	/**
	 * The partial sum of covariances over the points encountered so far. This
	 * is stored in the jblas DoubleMatrix class.
	 */
	private DoubleMatrix sigma_k_partial;

	/**
	 * The partial sum of means over the points encountered so far. This is
	 * stored in the JAMA Matrix class.
	 */
	private Matrix jama_mu_k_partial;

	/**
	 * The partial sum of covariances over the points encountered so far. This
	 * is stored in the JAMA Matrix class.
	 */
	private Matrix jama_sigma_k_partial;

	/**
	 * The number of points encountered so far.
	 */
	private int nPoints;

	/**
	 * A boolean flag indicating whether the final parameters have been computed
	 * or not.
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
	public PartialState(String matrixLibrary) {
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

	public void addPoint(double[][] xArray, double r_ik) {
		nPoints += 1;

		//
		// // Do updates
		r_k_partial += r_ik;

		if (matrixLibrary.equals("jblas")) {
			DoubleMatrix x = new DoubleMatrix(xArray);
			mu_k_partial = mu_k_partial.add(x.mul(r_ik));
			sigma_k_partial = sigma_k_partial.add(x.mmul(x.transpose()).mul(
					r_ik));
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
	public double[] getPartialStateDump() {
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
	public void addPartialStateDump(double[] partialStateDump) {

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
	public double getFinalAmplitude() {
		return r_k / nPoints;
	}

	/**
	 * 
	 * @return double array of the vector representation
	 */
	public double[][] getFinalMu() {
		return muFinalArray;
	}

	/**
	 * 
	 * @return double array of the matrix representation
	 */
	public double[][] getFinalSigma() {
		return sigmaFinalArray;
	}

	public void computeFinalParameters() {
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

	/**
	 * @return
	 */
	public MyriaMatrix[] getPartialStateDumpNewType() {
		// ONLY FOR JAMA

		MyriaMatrix[] partialStateNewType = new MyriaMatrix[1 + 1 + 1 + 1];

		MyriaMatrix partNPoints = new MyriaMatrix(1, 1, nPoints);
		MyriaMatrix partRK = new MyriaMatrix(1, 1, r_k_partial);
		MyriaMatrix muPart = new MyriaMatrix(jama_mu_k_partial);
		MyriaMatrix sigmaPart = new MyriaMatrix(jama_sigma_k_partial);

		// Fill the partial state array
		int indexCounter = 0;

		partialStateNewType[indexCounter] = partNPoints;
		indexCounter++;

		partialStateNewType[indexCounter] = partRK;
		indexCounter++;

		partialStateNewType[indexCounter] = muPart;
		indexCounter++;

		partialStateNewType[indexCounter] = sigmaPart;
		indexCounter++;

		return partialStateNewType;
	}

	// ONLY FOR JAMA
	public void addPartialStateDumpNewType(MyriaMatrix[] partialStateDump) {

		// Build the data structures we need
		double partNPoints;
		double partRK;

		// Convert from dump to parameters
		int indexCounter = 0;
		partNPoints = partialStateDump[indexCounter].get(0, 0);
		indexCounter++;
		partRK = partialStateDump[indexCounter].get(0, 0);
		indexCounter++;

		// Create matrices and add them to the current state
		Matrix jama_mu_k_from_dump = partialStateDump[indexCounter];
		indexCounter++;

		Matrix jama_sigma_k_from_dump = partialStateDump[indexCounter];
		indexCounter++;

		nPoints += partNPoints;
		r_k_partial += partRK;
		jama_mu_k_partial = jama_mu_k_partial.plus(jama_mu_k_from_dump);
		jama_sigma_k_partial = jama_sigma_k_partial
				.plus(jama_sigma_k_from_dump);
	}

}

/**
 *
 */
package edu.washington.escience.myria;

import Jama.Matrix;

/**
 * 
 */
public class MyriaMatrix extends Matrix implements Comparable {

	/** */
	private static final long serialVersionUID = 1L;

	/**
	 * Calls constructor of super class.
	 * 
	 * @param A
	 */
	public MyriaMatrix(double[][] A) {
		super(A);
	}

	/**
	 * This is a method to handle reading from sqlite and jdbc, which is not yet
	 * supported. By defaults creates a 1x1 matrix with a long value.
	 * 
	 * @param columnLong
	 */
	public MyriaMatrix(long columnLong) {
		super(0, 0, columnLong);
	}

	/**
	 * Dummy compare operator, since there's no good basis on which to compare
	 * arbitrary matrices.
	 * 
	 * @Override
	 * 
	 * @return less than 1 if this is less than o
	 */
	@Override
	public int compareTo(Object o) {
		return 0;
	}

}

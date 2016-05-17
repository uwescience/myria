package edu.washington.escience.myria.util;

/**
 * Math util methods.
 * */
public final class MathUtils {

  /**
   * Compute the number of 1s in a integer when written in binary.
   *
   * @param v the integer.
   * @return the number of 1s.
   * */
  public static int numBinaryOnesInInteger(final int v) {
    int result = 0;
    int value = v;
    while (value != 0) {
      result += (value & 0x1);
      value = value >>> 1;
    }
    return result;
  }

  /**
   * Safely cast a double to a float.
   *
   * @param v the double
   * @return the casted float of <code>v</code>
   * @throws ArithmeticException if the <code> v </code> is out of range.
   */
  public static float castDoubleToFloat(final double v) throws ArithmeticException {
    if (Math.abs(v) > Float.MAX_VALUE) {
      throw new ArithmeticException("casted value is out of the range of float");
    }
    return (float) v;
  }

  /**
   * util classes are not instantiable.
   * */
  private MathUtils() {}
}

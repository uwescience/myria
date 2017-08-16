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
   * Cast a long to a boolean.
   *
   * @param v the long
   * @returns True if <code>v != 0</code>, False otherwise
   */
  public static boolean castLongToBoolean(final long v) {
    return v != 0;
  }

  /**
   * Cast an int to a boolean.
   *
   * @param v the int
   * @returns True if <code>v != 0</code>, False otherwise
   */
  public static boolean castIntToBoolean(final int v) {
    return v != 0;
  }

  /**
   * Cast a boolean to an int
   *
   * @param v the boolean
   * @returns 1 if v was True, 0 if v was False
   */
  public static int castBooleanToInt(final boolean v) {
    if (v) {
      return 1;
    }
    return 0;
  }

  /**
   * Cast a boolean to an long
   *
   * @param v the boolean
   * @returns 1 if v was True, 0 if v was False
   */
  public static long castBooleanToLong(final boolean v) {
    if (v) {
      return 1L;
    }
    return 0L;
  }

  /**
   * util classes are not instantiable.
   * */
  private MathUtils() {}
}

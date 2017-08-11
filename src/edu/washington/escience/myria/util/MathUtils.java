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
   * @returns True if <code>v == 1</code>, False if <code>v == 0</code>
   * @throws IllegalArgumentException if v is anything but 1 or 0
   */
  public static boolean castLongToBoolean(final long v) {
    if (v == 0) {
      return false;
    } else if (v == 1) {
      return true;
    } else {
      throw new IllegalArgumentException("numbers other than 1 or 0 cannot be cast to boolean");
    }
  }

  /**
   * Cast an int to a boolean.
   *
   * @param v the int
   * @returns True if <code>v == 1</code>, False if <code>v == 0</code>
   * @throws IllegalArgumentException if v is anything but 1 or 0
   */
  public static boolean castIntToBoolean(final int v) {
    if (v == 0) {
      return false;
    } else if (v == 1) {
      return true;
    } else {
      throw new IllegalArgumentException("numbers other than 1 or 0 cannot be cast to boolean");
    }
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
   * util classes are not instantiable.
   * */
  private MathUtils() {}
}

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
   * util classes are not instantiable.
   * */
  private MathUtils() {
  }

}

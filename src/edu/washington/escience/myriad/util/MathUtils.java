package edu.washington.escience.myriad.util;

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
  public static int numBinaryOnesInInteger(int v) {
    int result = 0;
    while (v != 0) {
      result += (v & 0x01);
      v = v >>> 1;
    }
    return result;
  }

  /**
   * util classes are not instantiable.
   * */
  private MathUtils() {
  }

}
